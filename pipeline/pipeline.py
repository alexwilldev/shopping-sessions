#!/usr/bin/env python3
"""Shopping Sessions v1: DuckDB. Follows rules.md step by step.

Reads $DATA_DIR, writes $OUTPUT_DIR/{user_daily,campaign_daily}/ as Parquet.
Lines are split by lines.py (only "\n" ends a line, header skipped per file);
every field rule is applied here in SQL.
"""
import json
import os
import sys
import time

import duckdb
import numpy as np
import pyarrow as pa
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import connected_components

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lines import read_lines  # noqa: E402

DATA = os.environ.get("DATA_DIR", "/data")
OUT = os.environ.get("OUTPUT_DIR", "/output")
SCRATCH = os.environ.get("SCRATCH_DIR", "/scratch")
NCPU = int(os.environ.get("NCPU", os.cpu_count() or 1))
MEM_GB = int(os.environ.get("MEM_GB", "8"))

DAY = 86_400_000
WEEK = 7 * DAY
T0 = time.time()


def log(msg):
    print(f"[{time.time() - T0:7.2f}s] {msg}", file=sys.stderr, flush=True)


# One CSV field: wrapped in quotes with no quote/comma inside, or bare with no quote/comma.
FIELD = r'("[^",\n]*"|[^",\n]*)'


def line_regex(n):
    # The whole line (with its trailing \n) must match; group "all" is non-empty on a match
    # because a line of n >= 2 fields always holds a comma.
    return "^(" + ",".join([FIELD] * n) + ")\n$"


def parse_sql(table, names):
    groups = ["all"] + names
    pat = line_regex(len(names))
    sel = ", ".join(f"uq(m['{c}']) AS {c}" for c in names)
    return f"""
        SELECT {sel} FROM (
            SELECT regexp_extract(line, '{pat}', {groups!r}) AS m FROM {table}
        ) WHERE m['all'] <> ''
    """


MACROS = r"""
-- Quotes are syntax: strip a wrapping pair; an empty field (quoted or not) is NULL.
CREATE MACRO uq(f) AS NULLIF(CASE WHEN starts_with(f, '"') THEN f[2:-2] ELSE f END, '');

CREATE MACRO days_in_month(y, m) AS CASE
    WHEN m IN (1, 3, 5, 7, 8, 10, 12) THEN 31
    WHEN m IN (4, 6, 9, 11) THEN 30
    WHEN (y % 4 = 0 AND y % 100 <> 0) OR y % 400 = 0 THEN 29
    ELSE 28 END;

-- Howard Hinnant's days_from_civil (as in the generator); years here are >= 0.
CREATE MACRO days_from_civil(y, m, d) AS (
    (y - CASE WHEN m <= 2 THEN 1 ELSE 0 END) // 400 * 146097
    + ((y - CASE WHEN m <= 2 THEN 1 ELSE 0 END) % 400) * 365
    + ((y - CASE WHEN m <= 2 THEN 1 ELSE 0 END) % 400) // 4
    - ((y - CASE WHEN m <= 2 THEN 1 ELSE 0 END) % 400) // 100
    + (153 * ((m + 9) % 12) + 2) // 5 + d - 1
    - 719468);

-- F1 / F2 calendar form, already known to match the shape regex.
CREATE MACRO civil_ms(y, mo, d, h, mi, se, off_min, oh, om) AS CASE
    WHEN mo BETWEEN 1 AND 12 AND d BETWEEN 1 AND days_in_month(y, mo)
         AND h <= 23 AND mi <= 59 AND se <= 59 AND oh <= 23 AND om <= 59
    THEN (days_from_civil(y, mo, d)::BIGINT * 86400 + h * 3600 + mi * 60 + se) * 1000 - off_min::BIGINT * 60000
    ELSE NULL END;

-- F3: ASCII digits only, leading zeros allowed; too long for 64 bits is just out of window.
CREATE MACRO f3_ms(s) AS CASE
    WHEN ltrim(s, '0') = '' THEN 0::BIGINT
    WHEN length(ltrim(s, '0')) <= 18 THEN TRY_CAST(ltrim(s, '0') AS BIGINT)
    ELSE NULL END;

CREATE MACRO ts_ms(s) AS CASE
    WHEN s IS NULL THEN NULL
    WHEN regexp_full_match(s, '[0-9]+') THEN f3_ms(s)
    WHEN regexp_full_match(s, '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z') THEN
        civil_ms(TRY_CAST(s[1:4] AS INT), TRY_CAST(s[6:7] AS INT), TRY_CAST(s[9:10] AS INT),
                 TRY_CAST(s[12:13] AS INT), TRY_CAST(s[15:16] AS INT), TRY_CAST(s[18:19] AS INT), 0, 0, 0)
    WHEN regexp_full_match(s, '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[+-][0-9]{2}:[0-9]{2}') THEN
        civil_ms(TRY_CAST(s[1:4] AS INT), TRY_CAST(s[6:7] AS INT), TRY_CAST(s[9:10] AS INT),
                 TRY_CAST(s[12:13] AS INT), TRY_CAST(s[15:16] AS INT), TRY_CAST(s[18:19] AS INT),
                 (CASE WHEN s[20] = '-' THEN -1 ELSE 1 END)
                   * (TRY_CAST(s[21:22] AS INT) * 60 + TRY_CAST(s[24:25] AS INT)),
                 TRY_CAST(s[21:22] AS INT), TRY_CAST(s[24:25] AS INT))
    ELSE NULL END;

-- Purchase price: digits only, value 1..10,000,000 (leading zeros allowed).
CREATE MACRO price_ok(p) AS p IS NOT NULL AND regexp_full_match(p, '[0-9]+')
    AND length(ltrim(p, '0')) BETWEEN 1 AND 8
    AND TRY_CAST(ltrim(p, '0') AS BIGINT) <= 10000000;
"""


def main():
    os.makedirs(SCRATCH, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET threads = {NCPU}")
    con.execute(f"SET memory_limit = '{max(1, int(MEM_GB * 0.6))}GB'")
    con.execute(f"SET temp_directory = '{os.path.join(SCRATCH, 'duckdb_tmp')}'")
    con.execute("SET preserve_insertion_order = false")
    con.execute(MACROS)

    with open(os.path.join(DATA, "manifest.json")) as f:
        manifest = json.load(f)
    ws = con.execute("SELECT ts_ms(?)", [manifest["window_start"]]).fetchone()[0]
    we = con.execute("SELECT ts_ms(?)", [manifest["window_end"]]).fetchone()[0]
    log(f"window {ws}..{we}")

    ev_lines = read_lines(os.path.join(DATA, "events"), NCPU)
    ck_lines = read_lines(os.path.join(DATA, "clicks"), NCPU)
    lk_lines = read_lines(os.path.join(DATA, "links"), NCPU)
    pr_lines = read_lines(os.path.join(DATA, "products.csv"), NCPU)
    cp_lines = read_lines(os.path.join(DATA, "campaigns.csv"), NCPU)
    for name, tbl in [("ev_lines", ev_lines), ("ck_lines", ck_lines), ("lk_lines", lk_lines),
                      ("pr_lines", pr_lines), ("cp_lines", cp_lines)]:
        con.register(name, tbl)
    log(f"lines: events {ev_lines.num_rows}, clicks {ck_lines.num_rows}, links {lk_lines.num_rows}")

    con.execute(f"CREATE TABLE products AS {parse_sql('pr_lines', ['product_id', 'category'])}")
    con.execute(f"CREATE TABLE campaigns AS {parse_sql('cp_lines', ['campaign_id', 'channel'])}")

    # ---- Linked IDs: iterative connected components, name = byte-wise smallest ID.
    con.execute(f"""CREATE TABLE links AS SELECT user_id_a AS a, user_id_b AS b
        FROM ({parse_sql('lk_lines', ['user_id_a', 'user_id_b'])})
        WHERE user_id_a IS NOT NULL AND user_id_b IS NOT NULL""")
    con.execute("""CREATE TABLE lid AS SELECT id, (row_number() OVER () - 1)::BIGINT AS ix
        FROM (SELECT a AS id FROM links UNION SELECT b FROM links)""")
    n_ids = con.execute("SELECT count(*) FROM lid").fetchone()[0]
    pairs = con.execute("""SELECT la.ix AS i, lb.ix AS j FROM links
        JOIN lid la ON la.id = links.a JOIN lid lb ON lb.id = links.b""").to_arrow_table()
    if n_ids:
        i = pairs["i"].to_numpy()
        j = pairs["j"].to_numpy()
        graph = coo_matrix((np.ones(len(i), np.int8), (i, j)), shape=(n_ids, n_ids))
        _, comp = connected_components(graph, directed=False)
    else:
        comp = np.empty(0, np.int64)
    con.register("comp_tbl", pa.table({"ix": np.arange(n_ids, dtype=np.int64), "comp": comp.astype(np.int64)}))
    con.execute("""CREATE TABLE idmap AS SELECT id, min(id) OVER (PARTITION BY comp) AS person
        FROM lid JOIN comp_tbl USING (ix)""")
    log(f"links: {n_ids} ids")

    # ---- Events: parse -> dedup (smallest ingest_ts) -> validate -> drop late.
    con.execute(f"""CREATE TABLE ev_raw AS
        SELECT event_id, user_id, ts, ts_ms(ingest_ts) AS ingest, event_type, product_id, price_cents
        FROM ({parse_sql('ev_lines', ['event_id', 'user_id', 'ts', 'ingest_ts', 'event_type', 'product_id', 'price_cents'])})
        WHERE event_id IS NOT NULL""")
    con.execute("""CREATE TABLE ev_kept AS
        SELECT event_id, arg_min(struct_pack(user_id, ts, ingest, event_type, product_id, price_cents), ingest) AS r
        FROM ev_raw GROUP BY event_id""")
    con.execute(f"""CREATE TABLE ev AS
        SELECT coalesce(idmap.person, user_id) AS person, ts, event_type, product_id,
               CASE WHEN event_type = 'purchase' THEN TRY_CAST(ltrim(price_cents, '0') AS BIGINT) END AS price
        FROM (
            SELECT r.user_id AS user_id, ts_ms(r.ts) AS ts, r.ingest AS ingest, r.event_type AS event_type,
                   r.product_id AS product_id, r.price_cents AS price_cents
            FROM ev_kept
        ) e LEFT JOIN idmap ON idmap.id = e.user_id
        WHERE user_id IS NOT NULL AND ts IS NOT NULL AND ts >= {ws} AND ts < {we}
          AND event_type IN ('view', 'cart', 'purchase') AND product_id IS NOT NULL
          AND (event_type <> 'purchase' OR price_ok(price_cents))
          AND ingest - ts <= {DAY}""")
    log(f"events kept: {con.execute('SELECT count(*) FROM ev').fetchone()[0]}")

    # ---- Sessionize per person: new session when the gap to the previous event is > 30 min.
    # RANGE frame (not ROWS): rows with equal ts are peers and get the same running sum, so
    # tied events always share a session even though lag() and sum() may order ties differently.
    con.execute("""CREATE TABLE ev_s AS
        SELECT person, ts, event_type, product_id, price,
               sum(CASE WHEN prev IS NULL OR ts - prev > 1800000 THEN 1 ELSE 0 END)
                   OVER (PARTITION BY person ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sno
        FROM (SELECT *, lag(ts) OVER (PARTITION BY person ORDER BY ts) AS prev FROM ev)""")
    con.execute("""CREATE TABLE ev_ss AS
        SELECT person, min(ts) OVER (PARTITION BY person, sno) AS start, event_type, product_id, price
        FROM ev_s""")
    con.execute(f"""CREATE TABLE sessions AS
        SELECT person, start, (DATE '1970-01-01' + CAST(start // {DAY} AS INTEGER)) AS day,
               count(*) AS n_events,
               coalesce(sum(price), 0)::BIGINT AS revenue,
               bool_or(event_type = 'purchase') AS has_purchase
        FROM ev_ss GROUP BY person, start""")
    log(f"sessions: {con.execute('SELECT count(*) FROM sessions').fetchone()[0]}")

    # ---- user_daily
    con.execute(f"""CREATE TABLE user_daily AS
        WITH cat AS (
            SELECT person, (DATE '1970-01-01' + CAST(start // {DAY} AS INTEGER)) AS day,
                   coalesce(p.category, 'unknown') AS category, sum(price) AS rev
            FROM ev_ss LEFT JOIN products p USING (product_id)
            WHERE event_type = 'purchase' GROUP BY ALL
        ), top AS (
            SELECT person, day, first(category ORDER BY rev DESC, category ASC) AS top_category
            FROM cat GROUP BY person, day
        )
        SELECT s.person AS user_id, s.day, count(*)::BIGINT AS sessions, sum(n_events)::BIGINT AS events,
               sum(revenue)::BIGINT AS revenue_cents, any_value(top.top_category) AS top_category
        FROM sessions s LEFT JOIN top USING (person, day)
        GROUP BY s.person, s.day""")

    # ---- Clicks: parse -> dedup -> validate.
    con.execute(f"""CREATE TABLE clicks AS
        SELECT coalesce(idmap.person, c.user_id) AS person, c.ts, c.campaign_id, c.click_id
        FROM (
            SELECT click_id, any_value(user_id) AS user_id, ts_ms(any_value(ts)) AS ts,
                   any_value(campaign_id) AS campaign_id
            FROM ({parse_sql('ck_lines', ['click_id', 'user_id', 'campaign_id', 'ts'])})
            WHERE click_id IS NOT NULL GROUP BY click_id
        ) c
        JOIN campaigns USING (campaign_id)
        LEFT JOIN idmap ON idmap.id = c.user_id
        WHERE c.user_id IS NOT NULL AND c.ts IS NOT NULL AND c.ts >= {ws - WEEK} AND c.ts < {we}""")
    log(f"clicks kept: {con.execute('SELECT count(*) FROM clicks').fetchone()[0]}")

    # ---- Credit: latest click at or before the start (ties: smallest click_id), within 7 days.
    con.execute("""CREATE TABLE click_best AS
        SELECT person, ts, arg_min(campaign_id, click_id) AS campaign_id FROM clicks GROUP BY person, ts""")
    con.execute(f"""CREATE TABLE credited AS
        SELECT s.person, s.start, s.day, s.revenue, s.has_purchase,
               CASE WHEN c.ts IS NOT NULL AND s.start - c.ts <= {WEEK} THEN c.campaign_id ELSE 'organic' END AS campaign_id
        FROM sessions s ASOF LEFT JOIN click_best c ON s.person = c.person AND s.start >= c.ts""")

    # ---- Assists: each (person, campaign)'s clicks cover [ts, ts + 7d]; merge overlapping
    # coverage, cut it at day boundaries, and count the person's session starts in each piece.
    con.execute(f"""CREATE TABLE cover AS
        WITH c AS (SELECT DISTINCT person, campaign_id, ts FROM clicks),
        g AS (SELECT *, sum(CASE WHEN prev IS NULL OR ts - prev > {WEEK} THEN 1 ELSE 0 END)
                  OVER (PARTITION BY person, campaign_id ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp
              FROM (SELECT *, lag(ts) OVER (PARTITION BY person, campaign_id ORDER BY ts) AS prev FROM c))
        SELECT person, campaign_id, min(ts) AS lo, max(ts) + {WEEK} AS hi FROM g GROUP BY person, campaign_id, grp""")
    con.execute(f"""CREATE TABLE pieces AS
        SELECT person, campaign_id, d,
               greatest(lo, d * {DAY}) AS a, least(hi, d * {DAY} + {DAY - 1}) AS b
        FROM cover, LATERAL (SELECT unnest(range(lo // {DAY}, hi // {DAY} + 1)) AS d)
        WHERE d * {DAY} < {we} AND d * {DAY} + {DAY} > {ws}""")
    con.execute("""CREATE TABLE srank AS
        SELECT person, start, row_number() OVER (PARTITION BY person ORDER BY start) AS rk FROM sessions""")
    con.execute("""CREATE TABLE assists AS
        SELECT campaign_id, (DATE '1970-01-01' + CAST(d AS INTEGER)) AS day, sum(cnt)::BIGINT AS assisted
        FROM (
            SELECT p.campaign_id, p.d, coalesce(rb.rk, 0) - coalesce(ra.rk, 0) AS cnt
            FROM pieces p
            ASOF LEFT JOIN srank rb ON p.person = rb.person AND p.b >= rb.start
            ASOF LEFT JOIN srank ra ON p.person = ra.person AND p.a - 1 >= ra.start
        ) WHERE cnt > 0 GROUP BY ALL""")

    con.execute("""CREATE TABLE campaign_daily AS
        WITH cr AS (
            SELECT campaign_id, day, count(*)::BIGINT AS sessions,
                   count(*) FILTER (WHERE has_purchase)::BIGINT AS purchasing_sessions,
                   sum(revenue)::BIGINT AS revenue_cents
            FROM credited GROUP BY campaign_id, day)
        SELECT campaign_id,
               CASE WHEN campaign_id = 'organic' THEN 'organic' ELSE cp.channel END AS channel,
               day,
               coalesce(cr.sessions, 0)::BIGINT AS sessions,
               coalesce(cr.purchasing_sessions, 0)::BIGINT AS purchasing_sessions,
               coalesce(cr.revenue_cents, 0)::BIGINT AS revenue_cents,
               coalesce(a.assisted, 0)::BIGINT AS assisted_sessions
        FROM cr FULL OUTER JOIN assists a USING (campaign_id, day)
        LEFT JOIN campaigns cp USING (campaign_id)""")

    for table in ("user_daily", "campaign_daily"):
        folder = os.path.join(OUT, table)
        os.makedirs(folder, exist_ok=True)
        con.execute(f"COPY {table} TO '{os.path.join(folder, 'part-0.parquet')}' (FORMAT parquet)")
    log("done")


if __name__ == "__main__":
    main()
