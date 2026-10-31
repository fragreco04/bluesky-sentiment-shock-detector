"""Misura del volume in ambito (proposta §3.3): job BATCH su Kafka.

Applica al topic `posts` le STESSE funzioni di Q1 (lettura, termini, ambito), con il
dizionario indicato da GAMING_TERMS_PATH. Dizionari diversi si confrontano cosi' sugli
stessi dati, senza toccare Q1 ne' posts.enriched.

Per ogni finestra di MEASURE_WINDOW_MIN minuti interamente coperta dai dati:
  - copertura: quota di secondi con almeno un post inglese (rivela i buchi della raccolta)
  - post e autori inglesi, post e autori in ambito
  - quanti termini hanno >= 1 / 2 / 3 / 5 / 10 / 15 autori distinti: >= 1 e >= 2 dimensionano
    l'uscita di Q2, le soglie piu' alte servono a scegliere c_min (Q4)
Poi il riepilogo sulle finestre valide e i termini che superano le soglie piu' spesso.

Lancio, dalla cartella del progetto:
    docker compose run --rm --no-deps spark /opt/spark/bin/spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 \
      --conf spark.jars.ivy=/tmp/ivy \
      /opt/spark/jobs/tools/measure_scope.py
Con un altro dizionario o un'altra finestra, subito dopo "run":
    -e GAMING_TERMS_PATH=/opt/spark/config/<file>.json -e MEASURE_WINDOW_MIN=30
"""
import os
import sys
from datetime import datetime, timezone

# Rende importabile q1_enrich.py, che sta nella cartella superiore
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from q1_enrich import (GAMING_TERMS_PATH, add_scope, add_terms, build_text_pipeline,
                       content_col, load_dictionary, parse_posts)

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
WINDOW_MIN = int(os.getenv("MEASURE_WINDOW_MIN", "15"))
COVERAGE_MIN = float(os.getenv("MEASURE_COVERAGE_MIN", "0.98"))
THRESHOLDS = [1, 2, 3, 5, 10, 15]   # 1 = termini distinti per finestra (righe che Q2 emetterebbe)
RANK_BY = 5        # la classifica dei termini e' ordinata per finestre con >= RANK_BY autori
TOP_N = 30


def utc(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def main() -> None:
    spark = (SparkSession.builder.appName("measure_scope")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    window_s = WINDOW_MIN * 60

    pipeline = build_text_pipeline()
    dictionary = load_dictionary(spark, pipeline, GAMING_TERMS_PATH)

    raw = (spark.read.format("kafka")                         # batch: il topic letto come tabella
           .option("kafka.bootstrap.servers", BOOTSTRAP)
           .option("subscribe", "posts")
           .option("startingOffsets", "earliest")
           .option("endingOffsets", "latest")
           .load())

    posts = parse_posts(raw).dropDuplicates(["uri"])          # in batch la deduplica e' esatta
    scoped = add_scope(add_terms(posts.withColumn("content", content_col()), pipeline, text_col="content"),
                       dictionary)

    # Solo le colonne che servono, calcolate una volta e tenute in cache:
    # senza persist ogni aggregazione rileggerebbe Kafka e ritokenizzerebbe tutto.
    base = (scoped
            .select(F.window("event_time", f"{WINDOW_MIN} minutes").getField("start").alias("start"),
                    "event_time", "did", "in_scope",
                    F.when(F.col("in_scope"), F.col("terms")).alias("terms"))
            .persist())

    b = base.agg(F.min(F.unix_seconds("event_time")).alias("t0"),
                 F.max(F.unix_seconds("event_time")).alias("t1"),
                 F.count("*").alias("n"),
                 F.sum(F.col("in_scope").cast("int")).alias("n_scope")).first()
    if not b["n"]:
        print("Il topic posts e' vuoto.")
        spark.stop()
        return

    print(f"\nDizionario: {GAMING_TERMS_PATH}   finestra: {WINDOW_MIN} min")
    print(f"Dati da {utc(b['t0'])} a {utc(b['t1'])}")
    print(f"Post inglesi: {b['n']}   in ambito: {b['n_scope']} ({100 * b['n_scope'] / b['n']:.2f}%)")

    # Griglia delle finestre complete, calcolata in secondi dall'epoca (niente fusi orari).
    # F.window allinea le finestre all'epoca: la prima completa inizia al primo multiplo
    # di window_s dopo t0, l'ultima finisce all'ultimo multiplo prima di t1.
    first = -(-b["t0"] // window_s) * window_s                # arrotondamento per eccesso
    last_end = (b["t1"] // window_s) * window_s               # arrotondamento per difetto
    starts = list(range(first, last_end, window_s))
    if not starts:
        print("Dati insufficienti: nessuna finestra completa.")
        spark.stop()
        return
    grid = (spark.createDataFrame([(s,) for s in starts], "s LONG")
            .select(F.timestamp_seconds("s").alias("start")))

    # 1) Una sola aggregazione per i totali. countDistinct ignora i null:
    #    F.when(in_scope, did) conta solo gli autori in ambito.
    totals = base.groupBy("start").agg(
        F.count("*").alias("posts_en"),
        F.countDistinct("did").alias("authors_en"),
        F.countDistinct(F.date_trunc("second", "event_time")).alias("seconds"),
        F.sum(F.col("in_scope").cast("int")).alias("posts_scope"),
        F.countDistinct(F.when(F.col("in_scope"), F.col("did"))).alias("authors_scope"))

    # 2) Autori distinti per (finestra, termine), solo finestre complete
    per_term = (base.filter("in_scope")
                .select("start", "did", F.explode("terms").alias("term"))
                .groupBy("start", "term")
                .agg(F.countDistinct("did").alias("authors"))
                .join(grid, "start")
                .persist())
    term_counts = per_term.groupBy("start").agg(
        *[F.sum((F.col("authors") >= k).cast("int")).alias(f"terms_ge{k}") for k in THRESHOLDS])

    # La griglia a sinistra fa comparire anche le finestre senza alcun post (buchi interi),
    # che un semplice groupBy non mostrerebbe affatto.
    report = (grid
              .join(totals, "start", "left")
              .join(term_counts, "start", "left")
              .fillna(0)
              .withColumn("coverage", F.round(F.col("seconds") / window_s, 3))
              # try_divide: con ANSI attivo una divisione per zero lancerebbe un'eccezione
              .withColumn("scope_pct", F.round(100 * F.try_divide(F.col("authors_scope"), F.col("authors_en")), 2))
              .withColumn("valid", F.col("coverage") >= COVERAGE_MIN)
              .orderBy("start")
              .persist())

    cols = ["posts_en", "authors_en", "coverage", "posts_scope", "authors_scope", "scope_pct"] \
        + [f"terms_ge{k}" for k in THRESHOLDS]
    print("\nPer finestra:")
    (report.select(F.date_format("start", "MM-dd HH:mm").alias("inizio"), *cols,
                   F.when(~F.col("valid"), "<- buco").otherwise("").alias("nota"))
           .show(1000, truncate=False))

    valid = report.filter("valid")
    n_valid = valid.count()
    print(f"Finestre complete: {len(starts)}   valide (copertura >= {COVERAGE_MIN:.0%}): {n_valid}")
    print("Riepilogo sulle finestre valide (mediana / minimo / massimo):")
    summary = ["authors_en", "authors_scope", "scope_pct"] + [f"terms_ge{k}" for k in THRESHOLDS]
    valid.select(*[F.concat_ws(" / ",
                               F.percentile_approx(c, 0.5).cast("string"),
                               F.min(c).cast("string"),
                               F.max(c).cast("string")).alias(c) for c in summary]
                 ).show(truncate=False)

    # 3) Quali termini superano le soglie: se sono solo parole generiche, c_min e' irraggiungibile
    #    proprio per i termini nuovi, che sono quelli che il sistema deve segnalare.
    print(f"Termini che raggiungono piu' spesso >= {RANK_BY} autori (finestre valide: {n_valid}):")
    (per_term.join(valid.select("start"), "start")
             .groupBy("term")
             .agg(*[F.sum((F.col("authors") >= k).cast("int")).alias(f"win_ge{k}") for k in THRESHOLDS],
                  F.max("authors").alias("max_authors"))
             .orderBy(F.desc(f"win_ge{RANK_BY}"), F.desc("max_authors"))
             .show(TOP_N, truncate=False))

    spark.stop()


if __name__ == "__main__":
    main()