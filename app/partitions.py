"""Backfill of missing daily RANGE partitions.

Contract (see README / endpoint docs)
-------------------------------------
* Only parent tables that are partitioned by RANGE on a single ``date``
  column, one partition level, and have **no DEFAULT partition** are
  accepted. Anything else is rejected up front (422), with no DDL run.
* The request covers an inclusive date range of at most
  ``config.PARTITION_MAX_DAYS`` days.
* An existing partition whose bounds are exactly ``[day, day+1)`` is
  *reused* for that day. A missing day gets a new partition.
* If any requested day already falls inside an existing partition whose
  bounds are NOT exactly ``[day, day+1)`` (a wider/straddling partition),
  the whole batch is rejected (409) and nothing is created.
* Concurrency with other API requests and hand-written DDL is serialized on
  the parent table via ``LOCK TABLE ... IN SHARE UPDATE EXCLUSIVE MODE``.
  ``CREATE TABLE ... PARTITION OF`` takes AccessExclusiveLock, which
  conflicts with it, so the same day can never get two partitions.
* Every batch runs in one transaction on its OWN short-lived connection
  (never on the two diagnostic long connections). Any conflict or creation
  failure rolls the whole batch back: no partial leftovers. A retry can
  tell pre-existing partitions ("reused") apart from the ones it creates
  ("created"), because the catalogs are read again inside the same lock.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date, timedelta

import psycopg
from psycopg.sql import SQL, Identifier, Literal

from . import config

log = logging.getLogger("diag.partitions")

_DAY = timedelta(days=1)
# pg_get_expr renders date bounds as FOR VALUES FROM ('2026-11-20') TO (...).
_BOUND_RE = re.compile(
    r"^FOR VALUES FROM \('(\d{4}-\d{2}-\d{2})'\) TO \('(\d{4}-\d{2}-\d{2})'\)$"
)


class PartitionError(Exception):
    """Base class; ``status_code`` maps directly to the HTTP response."""

    status_code = 400


class PartitionValidationError(PartitionError):
    """Table/request does not satisfy the supported shape."""

    status_code = 422


class PartitionConflict(PartitionError):
    """Requested day is covered by an incompatible existing partition."""

    status_code = 409


class PartitionUnavailable(PartitionError):
    """Parent table is locked by another session right now; retry later."""

    status_code = 409


class PartitionCreationFailed(PartitionError):
    """DDL failed inside the transaction; the batch was rolled back."""

    status_code = 500


@dataclass(frozen=True)
class ExistingPartition:
    name: str
    lower: date
    upper: date


@dataclass(frozen=True)
class DayPlan:
    day: date
    # None  -> a new partition must be created
    # str   -> exact-match existing partition name to reuse
    reuse: str | None


def daterange(start: date, end_inclusive: date) -> list[date]:
    return [start + timedelta(days=i) for i in range((end_inclusive - start).days + 1)]


def _connect() -> psycopg.Connection:
    """Independent maintenance connection.

    psycopg defaults to ``autocommit=False`` (a transaction opens on the
    first command); we set it explicitly before any query is sent so the
    whole batch is one clearly-delimited transaction.
    """
    conn = psycopg.connect(
        config.DATABASE_URL,
        connect_timeout=config.CONNECT_TIMEOUT_S,
        prepare_threshold=None,
        autocommit=False,
    )
    return conn


def _load_table_shape(cur, schema: str, table: str) -> None:
    """Verify the supported partition shape; raise on every deviation.

    Runs inside the SUE lock, so the answer stays valid for the rest of the
    transaction (concurrent DDL cannot get through the same lock).
    """
    cur.execute(
        """
        SELECT c.relkind,
               c.relispartition,
               pt.partstrat,
               pt.partnatts,
               (SELECT count(*) FROM pg_partitioned_table pt2
                 WHERE pt2.partrelid IN (
                    SELECT i.inhrelid FROM pg_inherits i
                    WHERE i.inhparent = c.oid)) AS partitioned_children
          FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
          LEFT JOIN pg_partitioned_table pt ON pt.partrelid = c.oid
         WHERE n.nspname = %s AND c.relname = %s
        """,
        (schema, table),
    )
    row = cur.fetchone()
    if row is None:
        raise PartitionValidationError(
            f"table {schema}.{table} does not exist"
        )
    relkind, relispartition, partstrat, partnatts, partitioned_children = row
    if relkind != "p":
        raise PartitionValidationError(
            f"{schema}.{table} is not a partitioned table (relkind={relkind!r})"
        )
    if relispartition:
        raise PartitionValidationError(
            f"{schema}.{table} is itself a partition of another table; "
            "only single-level partitioned tables are supported"
        )
    if partstrat != "r":
        raise PartitionValidationError(
            f"{schema}.{table} is partitioned by {partstrat!r}, not RANGE; "
            "only single-column DATE RANGE partitioning is supported"
        )
    if partnatts != 1:
        raise PartitionValidationError(
            f"{schema}.{table} has {partnatts} partition keys; "
            "exactly one DATE column is required"
        )
    if partitioned_children:
        raise PartitionValidationError(
            f"{schema}.{table} has sub-partitioned children; "
            "only single-level partitioning is supported"
        )
    # The single partition key must be a plain date column. The parent OID
    # is fetched first so the unnest() lateral query stays self-contained
    # (it cannot reference the outer pg_partitioned_table alias).
    cur.execute(
        """
        SELECT c.oid
          FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = %s AND c.relname = %s
        """,
        (schema, table),
    )
    parent_oid = cur.fetchone()[0]
    cur.execute(
        """
        SELECT a.attname, format_type(a.atttypid, a.atttypmod)
          FROM (
              SELECT k.attnum, k.ord
                FROM pg_partitioned_table pt,
                     LATERAL unnest(pt.partattrs)
                       WITH ORDINALITY AS k(attnum, ord)
               WHERE pt.partrelid = %s
          ) keys
          JOIN pg_attribute a
            ON a.attrelid = %s AND a.attnum = keys.attnum
         ORDER BY keys.ord
        """,
        (parent_oid, parent_oid),
    )
    key_rows = cur.fetchall()
    if len(key_rows) != 1 or key_rows[0][1] != "date":
        got = ", ".join(f"{name}: {typ}" for name, typ in key_rows) or "none"
        raise PartitionValidationError(
            f"{schema}.{table}: partition key must be one DATE column; got {got}"
        )


def _load_partitions(cur, schema: str, table: str) -> list[ExistingPartition]:
    cur.execute(
        """
        SELECT child.relname, pg_get_expr(child.relpartbound, child.oid)
          FROM pg_inherits i
          JOIN pg_class child ON child.oid = i.inhrelid
         WHERE i.inhparent =
               (SELECT c.oid FROM pg_class c
                  JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname = %s AND c.relname = %s)
         ORDER BY child.relname
        """,
        (schema, table),
    )
    parts: list[ExistingPartition] = []
    for name, bound in cur.fetchall():
        if bound is None or bound.strip().upper() == "DEFAULT":
            # Must never happen for accepted tables (checked before locking
            # again here), but keep the guard explicit.
            raise PartitionValidationError(
                f"{schema}.{table} has a DEFAULT partition ({name}); "
                "backfill is only supported without one"
            )
        m = _BOUND_RE.match(bound.strip())
        if not m:
            raise PartitionValidationError(
                f"existing partition {name} has non-daily bounds that cannot "
                f"be interpreted: {bound}"
            )
        parts.append(
            ExistingPartition(
                name=name,
                lower=date.fromisoformat(m.group(1)),
                upper=date.fromisoformat(m.group(2)),
            )
        )
    return parts


def _has_default_partition(cur, schema: str, table: str) -> bool:
    cur.execute(
        """
        SELECT EXISTS (
          SELECT 1
            FROM pg_inherits i
            JOIN pg_class child ON child.oid = i.inhrelid
           WHERE i.inhparent =
                 (SELECT c.oid FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                   WHERE n.nspname = %s AND c.relname = %s)
             AND pg_get_expr(child.relpartbound, child.oid) = 'DEFAULT'
        )
        """,
        (schema, table),
    )
    return bool(cur.fetchone()[0])


def _plan(days: list[date], parts: list[ExistingPartition]) -> list[DayPlan]:
    """Decide per day: exact-match reuse, create, or whole-batch conflict."""
    plans: list[DayPlan] = []
    for day in days:
        next_day = day + _DAY
        covering = [p for p in parts if p.lower <= day < p.upper]
        if not covering:
            plans.append(DayPlan(day=day, reuse=None))
            continue
        if len(covering) == 1 and covering[0].lower == day and covering[0].upper == next_day:
            plans.append(DayPlan(day=day, reuse=covering[0].name))
            continue
        # A straddling/wider partition already routes this day elsewhere.
        names = ", ".join(sorted(p.name for p in covering))
        raise PartitionConflict(
            f"day {day.isoformat()} is already covered by existing "
            f"partition(s) [{names}] whose bounds are not exactly "
            f"[{day.isoformat()}, {next_day.isoformat()}); batch rejected "
            "without creating any partition"
        )
    return plans


def _partition_name(table: str, day: date, used: set[str]) -> str:
    """Deterministic, collision-free child name (<= 63 chars).

    ``used`` already contains every existing child name plus the names of
    partitions created earlier in this batch, so a numeric suffix resolves
    clashes with an existing child that deviates from the default pattern.
    """
    base = f"{table}_{day:%Y%m%d}"
    candidate = base
    suffix = 1
    while len(candidate) > config.MAX_IDENTIFIER_LEN or candidate in used:
        tag = f"_{suffix}"
        candidate = base[: config.MAX_IDENTIFIER_LEN - len(tag)] + tag
        suffix += 1
    used.add(candidate)
    return candidate


def backfill_partitions(schema: str, table: str, start: date, end: date) -> dict:
    """Create the missing daily partitions for ``start..end`` (inclusive).

    Returns a JSON-serializable summary. Raises :class:`PartitionError`
    subclasses on any rejection/failure; in every failure case the batch
    transaction is rolled back and no partition remains from this call.
    """
    if end < start:
        raise PartitionValidationError("end date must not be before start date")
    days = daterange(start, end)
    if len(days) > config.PARTITION_MAX_DAYS:
        raise PartitionValidationError(
            f"range spans {len(days)} days; at most "
            f"{config.PARTITION_MAX_DAYS} days per request"
        )

    conn = _connect()
    created: list[dict] = []
    reused: list[dict] = []
    try:
        # One transaction for the whole batch: all-or-nothing. The
        # connection was opened with autocommit=False, so the first command
        # below starts the transaction that commit()/rollback() finalize.
        with conn.cursor() as cur:
            # SET does not accept bind parameters; the values are our own
            # integer config, quoted safely via sql.Literal.
            cur.execute(
                SQL("SET LOCAL lock_timeout = {}").format(
                    Literal(f"{config.PARTITION_LOCK_TIMEOUT_MS}ms")
                )
            )
            cur.execute(
                SQL("SET LOCAL statement_timeout = {}").format(
                    Literal(f"{config.PARTITION_STATEMENT_TIMEOUT_MS}ms")
                )
            )
            try:
                # Serializes against hand-written DDL (AEL on the parent)
                # and against other backfill requests (same SUE lock).
                cur.execute(
                    SQL("LOCK TABLE {}.{} IN SHARE UPDATE EXCLUSIVE MODE").format(
                        Identifier(schema), Identifier(table)
                    )
                )
            except psycopg.errors.UndefinedTable:
                raise PartitionValidationError(
                    f"table {schema}.{table} does not exist"
                )
            except psycopg.errors.InvalidSchemaName:
                raise PartitionValidationError(f"schema {schema} does not exist")
            except psycopg.errors.LockNotAvailable:
                raise PartitionUnavailable(
                    f"could not lock {schema}.{table} within "
                    f"{config.PARTITION_LOCK_TIMEOUT_MS}ms; another session is "
                    "running DDL on it, retry later"
                )
            except psycopg.errors.QueryCanceled as exc:
                raise PartitionUnavailable(
                    f"timed out waiting for lock on {schema}.{table}: {exc}"
                )

            if _has_default_partition(cur, schema, table):
                raise PartitionValidationError(
                    f"{schema}.{table} has a DEFAULT partition; backfill is "
                    "only supported for tables without one"
                )
            _load_table_shape(cur, schema, table)
            parts = _load_partitions(cur, schema, table)
            plans = _plan(days, parts)

            used_names = {p.name for p in parts}
            for plan in plans:
                if plan.reuse is not None:
                    reused.append(
                        {"date": plan.day.isoformat(), "partition": plan.reuse}
                    )
                    continue
                name = _partition_name(table, plan.day, used_names)
                lower = plan.day
                upper = plan.day + _DAY
                try:
                    # FOR VALUES does not accept bind parameters; date values
                    # are rendered as safely quoted literals.
                    cur.execute(
                        SQL(
                            "CREATE TABLE {}.{} PARTITION OF {}.{} "
                            "FOR VALUES FROM ({}) TO ({})"
                        ).format(
                            Identifier(schema),
                            Identifier(name),
                            Identifier(schema),
                            Identifier(table),
                            Literal(lower),
                            Literal(upper),
                        )
                    )
                except psycopg.errors.LockNotAvailable:
                    raise PartitionUnavailable(
                        f"lock timeout while creating partition {name}; "
                        "whole batch rolled back, retry later"
                    )
                except psycopg.errors.DuplicateObject:
                    # Concurrent session won the race between our catalog
                    # read and this DDL. SUE locking makes this unlikely,
                    # but handle it as a batch-level conflict.
                    raise PartitionConflict(
                        f"partition for {lower.isoformat()} appeared "
                        "concurrently; batch rolled back, retry to reuse it"
                    )
                except psycopg.Error as exc:
                    raise PartitionCreationFailed(
                        f"failed to create partition {name} for "
                        f"[{lower.isoformat()}, {upper.isoformat()}): "
                        f"{type(exc).__name__}: {exc}".strip()
                    )
                created.append(
                    {
                        "date": lower.isoformat(),
                        "partition": name,
                        "bounds": {
                            "from": lower.isoformat(),
                            "to": upper.isoformat(),
                        },
                    }
                )
        conn.commit()
    except PartitionError:
        conn.rollback()
        raise
    except psycopg.errors.QueryCanceled as exc:
        conn.rollback()
        raise PartitionUnavailable(f"statement timeout: {exc}")
    except psycopg.Error as exc:
        # Anything that was not classified at its source statement (LOCK /
        # CREATE) is an internal failure; never guess "table missing" from a
        # SQLSTATE here, since a 42P01 inside a catalog query would mask the
        # real bug. The batch txn is rolled back either way.
        conn.rollback()
        raise PartitionCreationFailed(
            f"{type(exc).__name__} {getattr(exc, 'sqlstate', '')}: "
            f"{exc}".strip()
        )
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    log.info(
        "partition backfill %s.%s %s..%s: created=%d reused=%d",
        schema,
        table,
        start.isoformat(),
        end.isoformat(),
        len(created),
        len(reused),
    )
    return {
        "schema_name": schema,
        "table_name": table,
        "range": {"start": start.isoformat(), "end": end.isoformat()},
        "created": created,
        "reused": reused,
        "created_count": len(created),
        "reused_count": len(reused),
    }
