#!/usr/bin/env python3
"""Shopping Sessions v1b: Polars. Same rules and structure as pipeline.py (DuckDB).

Reads $DATA_DIR, writes $OUTPUT_DIR/{user_daily,campaign_daily}/ as Parquet.
"""
import json
import os
import sys
import time

import numpy as np
import polars as pl
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import connected_components

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lines import read_lines  # noqa: E402

DATA = os.environ.get("DATA_DIR", "/data")
OUT = os.environ.get("OUTPUT_DIR", "/output")
NCPU = int(os.environ.get("NCPU", os.cpu_count() or 1))

DAY = 86_400_000
WEEK = 7 * DAY
HALF_HOUR = 1_800_000
T0 = time.time()
ENGINE = "streaming"


def log(msg):
    print(f"[{time.time() - T0:7.2f}s] {msg}", file=sys.stderr, flush=True)


FIELD = r'("[^",\n]*"|[^",\n]*)'


def uq(e):
    """Quotes are syntax: strip a wrapping pair; an empty field (quoted or not) is null."""
    v = pl.when(e.str.starts_with('"')).then(e.str.slice(1, e.str.len_bytes() - 2)).otherwise(e)
    return pl.when(v == "").then(None).otherwise(v)


def parse(path, names):
    """LazyFrame of the well-formed lines of the CSV file(s), fields unquoted, empty -> null."""
    lines = pl.from_arrow(read_lines(path, NCPU)).lazy()
    pat = "^" + ",".join(f"(?P<{n}>{FIELD[1:-1]})" for n in names) + "\n$"
    return (lines.select(pl.col("line").str.extract_groups(pat).alias("m"))
            .unnest("m")
            .filter(pl.col(names[0]).is_not_null())  # no match -> all groups null
            .select([uq(pl.col(n)).alias(n) for n in names]))


def days_from_civil(y, m, d):
    yy = y - (m <= 2).cast(pl.Int64)
    era = yy // 400
    yoe = yy - era * 400
    doy = (153 * ((m + 9) % 12) + 2) // 5 + d - 1
    return era * 146097 + yoe * 365 + yoe // 4 - yoe // 100 + doy - 719468


def ts_ms(e):
    """Timestamp in F1 / F2 / F3 -> epoch ms, or null when bad (rules.md 'Timestamps')."""
    def num(a, b):
        return e.str.slice(a, b).cast(pl.Int64, strict=False)
    y, mo, d, h, mi, se = num(0, 4), num(5, 2), num(8, 2), num(11, 2), num(14, 2), num(17, 2)
    oh, om = num(20, 2), num(23, 2)
    leap = ((y % 4 == 0) & (y % 100 != 0)) | (y % 400 == 0)
    dim = (pl.when(mo.is_in([4, 6, 9, 11])).then(30)
           .when(mo == 2).then(pl.when(leap).then(29).otherwise(28))
           .otherwise(31))
    cal_ok = mo.is_between(1, 12) & d.is_between(1, dim) & (h <= 23) & (mi <= 59) & (se <= 59)
    local = (days_from_civil(y, mo, d) * 86400 + h * 3600 + mi * 60 + se) * 1000
    sign = pl.when(e.str.slice(19, 1) == "-").then(-1).otherwise(1)
    digits = e.str.strip_chars_start("0")
    return (pl.when(e.str.contains(r"^[0-9]+$"))
            .then(pl.when(digits == "").then(0)
                  .when(digits.str.len_bytes() <= 18).then(digits.cast(pl.Int64, strict=False))
                  .otherwise(None))
            .when(e.str.contains(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$") & cal_ok)
            .then(local)
            .when(e.str.contains(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[+-][0-9]{2}:[0-9]{2}$")
                  & cal_ok & (oh <= 23) & (om <= 59))
            .then(local - sign * (oh * 60 + om) * 60000)
            .otherwise(None)
            .cast(pl.Int64))


def price_ok(e):
    digits = e.str.strip_chars_start("0")
    return (e.str.contains(r"^[0-9]+$").fill_null(False)
            & digits.str.len_bytes().is_between(1, 8).fill_null(False)
            & (digits.cast(pl.Int64, strict=False) <= 10_000_000).fill_null(False))


def to_date(ms):
    return (ms // DAY).cast(pl.Int32).cast(pl.Date)


def main():
    with open(os.path.join(DATA, "manifest.json")) as f:
        manifest = json.load(f)
    win = pl.DataFrame({"s": [manifest["window_start"], manifest["window_end"]]}).select(ts_ms(pl.col("s")).alias("s"))
    ws, we = win["s"].to_list()
    log(f"window {ws}..{we}")

    products = parse(os.path.join(DATA, "products.csv"), ["product_id", "category"]).collect()
    campaigns = parse(os.path.join(DATA, "campaigns.csv"), ["campaign_id", "channel"]).collect()

    # ---- Linked IDs: iterative connected components, name = byte-wise smallest ID.
    links = (parse(os.path.join(DATA, "links"), ["user_id_a", "user_id_b"])
             .filter(pl.col("user_id_a").is_not_null() & pl.col("user_id_b").is_not_null())
             .collect(engine=ENGINE))
    ids = (pl.concat([links["user_id_a"], links["user_id_b"]]).unique().to_frame("id")
           .with_row_index("ix"))
    pairs = (links.join(ids.rename({"id": "user_id_a", "ix": "i"}), on="user_id_a")
             .join(ids.rename({"id": "user_id_b", "ix": "j"}), on="user_id_b"))
    n = ids.height
    if n:
        i, j = pairs["i"].to_numpy(), pairs["j"].to_numpy()
        _, comp = connected_components(coo_matrix((np.ones(len(i), np.int8), (i, j)), shape=(n, n)), directed=False)
    else:
        comp = np.empty(0, np.int64)
    idmap = (ids.with_columns(pl.Series("comp", comp))
             .with_columns(pl.col("id").min().over("comp").alias("person"))
             .select(pl.col("id").alias("user_id"), "person"))
    log(f"links: {n} ids")

    def person(df):
        return (df.join(idmap.lazy(), on="user_id", how="left")
                .with_columns(pl.coalesce("person", "user_id").alias("person")))

    # ---- Events: parse -> dedup (smallest ingest_ts) -> validate -> drop late.
    # ingest_ts is always valid F1 (rules.md), so the native parser is exact here (checked
    # identical to ts_ms on 20M rows) and ~8x faster.
    raw = (parse(os.path.join(DATA, "events"),
                 ["event_id", "user_id", "ts", "ingest_ts", "event_type", "product_id", "price_cents"])
           .filter(pl.col("event_id").is_not_null())
           .with_columns(pl.col("ingest_ts").str.to_datetime("%Y-%m-%dT%H:%M:%SZ", time_unit="ms", strict=False)
                         .dt.epoch("ms").alias("ingest"))
           .collect(engine=ENGINE))
    # Dedup before validation: only ids seen more than once need the "smallest ingest_ts" pick
    # (copies with equal ingest_ts are identical, so keep="first" after the sort is exact).
    dup = raw.select(pl.col("event_id").is_duplicated())["event_id"]
    kept = pl.concat([raw.filter(~dup),
                      raw.filter(dup).sort("ingest").unique("event_id", keep="first", maintain_order=True)])
    del raw
    ev = (kept.lazy()
          .with_columns(ts_ms(pl.col("ts")).alias("ts"))
          .filter(pl.col("user_id").is_not_null() & pl.col("ts").is_not_null()
                  & (pl.col("ts") >= ws) & (pl.col("ts") < we)
                  & pl.col("event_type").is_in(["view", "cart", "purchase"])
                  & pl.col("product_id").is_not_null()
                  & ((pl.col("event_type") != "purchase") | price_ok(pl.col("price_cents")))
                  & (pl.col("ingest") - pl.col("ts") <= DAY))
          .with_columns(pl.when(pl.col("event_type") == "purchase")
                        .then(pl.col("price_cents").str.strip_chars_start("0").cast(pl.Int64))
                        .alias("price"))
          .select("user_id", "ts", "event_type", "product_id", "price"))
    ev = person(ev).select("person", "ts", "event_type", "product_id", "price").collect(engine=ENGINE)
    log(f"events kept: {ev.height}")

    # ---- Sessionize: sort by (person, ts); a new session when the person changes or the gap > 30 min.
    ev = ev.sort("person", "ts")
    ev = ev.with_columns(
        ((pl.col("person") != pl.col("person").shift(1)).fill_null(True)
         | ((pl.col("ts") - pl.col("ts").shift(1)) > HALF_HOUR).fill_null(True)).cum_sum().alias("sid"))
    ev = ev.with_columns(pl.col("ts").first().over("sid").alias("start"))
    sessions = (ev.group_by("sid").agg(
        pl.col("person").first(), pl.col("start").first(), pl.len().alias("n_events"),
        pl.col("price").sum().fill_null(0).alias("revenue"),
        (pl.col("event_type") == "purchase").any().alias("has_purchase"))
        .with_columns(to_date(pl.col("start")).alias("day")))
    log(f"sessions: {sessions.height}")

    # ---- user_daily
    top = (ev.filter(pl.col("event_type") == "purchase")
           .join(products, on="product_id", how="left")
           .with_columns(pl.col("category").fill_null("unknown"), to_date(pl.col("start")).alias("day"))
           .group_by("person", "day", "category").agg(pl.col("price").sum().alias("rev"))
           .sort(["rev", "category"], descending=[True, False])
           .group_by("person", "day").agg(pl.col("category").first().alias("top_category")))
    user_daily = (sessions.group_by("person", "day").agg(
        pl.len().alias("sessions"), pl.col("n_events").sum().alias("events"),
        pl.col("revenue").sum().alias("revenue_cents"))
        .join(top, on=["person", "day"], how="left")
        .select(pl.col("person").alias("user_id"), "day",
                pl.col("sessions").cast(pl.Int64), pl.col("events").cast(pl.Int64),
                pl.col("revenue_cents").cast(pl.Int64), "top_category"))

    # ---- Clicks: parse -> dedup -> validate.
    ck = (parse(os.path.join(DATA, "clicks"), ["click_id", "user_id", "campaign_id", "ts"])
          .filter(pl.col("click_id").is_not_null())
          .unique("click_id", keep="any")
          .with_columns(ts_ms(pl.col("ts")).alias("ts"))
          .filter(pl.col("user_id").is_not_null() & pl.col("ts").is_not_null()
                  & (pl.col("ts") >= ws - WEEK) & (pl.col("ts") < we))
          .join(campaigns.lazy().select("campaign_id"), on="campaign_id", how="semi"))
    clicks = person(ck).select("person", "ts", "campaign_id", "click_id").collect(engine=ENGINE)
    log(f"clicks kept: {clicks.height}")

    # ---- Credit: latest click at or before the start (ties: smallest click_id), within 7 days.
    best = (clicks.sort("click_id").group_by("person", "ts").agg(pl.col("campaign_id").first())
            .sort("ts"))
    credited = (sessions.sort("start")
                .join_asof(best.rename({"ts": "cts"}), left_on="start", right_on="cts", by="person",
                           strategy="backward")
                .with_columns(pl.when(pl.col("cts").is_not_null() & (pl.col("start") - pl.col("cts") <= WEEK))
                              .then(pl.col("campaign_id")).otherwise(pl.lit("organic")).alias("campaign_id")))

    # ---- Assists: merge each (person, campaign)'s coverage [ts, ts+7d], cut it at day
    # boundaries, and count the person's session starts in each piece via session ranks.
    cover = (clicks.select("person", "campaign_id", "ts").unique().sort("person", "campaign_id", "ts")
             .with_columns((((pl.col("person") != pl.col("person").shift(1))
                             | (pl.col("campaign_id") != pl.col("campaign_id").shift(1))).fill_null(True)
                            | ((pl.col("ts") - pl.col("ts").shift(1)) > WEEK).fill_null(True))
                           .cum_sum().alias("grp"))
             .group_by("grp").agg(pl.col("person").first(), pl.col("campaign_id").first(),
                                  pl.col("ts").min().alias("lo"), (pl.col("ts").max() + WEEK).alias("hi")))
    pieces = (cover.with_columns(pl.int_ranges(pl.col("lo") // DAY, pl.col("hi") // DAY + 1).alias("d"))
              .explode("d")
              .filter((pl.col("d") * DAY < we) & (pl.col("d") * DAY + DAY > ws))
              .with_columns(pl.max_horizontal("lo", pl.col("d") * DAY).alias("a"),
                            pl.min_horizontal("hi", pl.col("d") * DAY + DAY - 1).alias("b")))
    srank = (sessions.select("person", "start").sort("person", "start")
             .with_columns(pl.int_range(1, pl.len() + 1).over("person").alias("rk"))
             .sort("start"))
    assists = (pieces.with_columns((pl.col("a") - 1).alias("a1")).sort("b")
               .join_asof(srank.rename({"start": "sb", "rk": "rb"}), left_on="b", right_on="sb", by="person",
                          strategy="backward")
               .sort("a1")
               .join_asof(srank.rename({"start": "sa", "rk": "ra"}), left_on="a1", right_on="sa", by="person",
                          strategy="backward")
               .with_columns((pl.col("rb").fill_null(0) - pl.col("ra").fill_null(0)).alias("cnt"))
               .filter(pl.col("cnt") > 0)
               .group_by("campaign_id", "d").agg(pl.col("cnt").sum().alias("assisted_sessions"))
               .with_columns(pl.col("d").cast(pl.Int32).cast(pl.Date).alias("day")).drop("d"))

    cr = credited.group_by("campaign_id", "day").agg(
        pl.len().alias("sessions"),
        pl.col("has_purchase").sum().alias("purchasing_sessions"),
        pl.col("revenue").sum().alias("revenue_cents"))
    campaign_daily = (cr.join(assists, on=["campaign_id", "day"], how="full", coalesce=True)
                      .join(campaigns, on="campaign_id", how="left")
                      .select("campaign_id",
                              pl.when(pl.col("campaign_id") == "organic").then(pl.lit("organic"))
                              .otherwise(pl.col("channel")).alias("channel"),
                              "day",
                              *[pl.col(c).fill_null(0).cast(pl.Int64) for c in
                                ("sessions", "purchasing_sessions", "revenue_cents", "assisted_sessions")]))

    for name, df in (("user_daily", user_daily), ("campaign_daily", campaign_daily)):
        folder = os.path.join(OUT, name)
        os.makedirs(folder, exist_ok=True)
        df.write_parquet(os.path.join(folder, "part-0.parquet"), compression="uncompressed")
    log("done")


if __name__ == "__main__":
    main()
