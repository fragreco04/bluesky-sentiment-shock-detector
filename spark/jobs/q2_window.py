"""
Q2 - Aggregazione a finestre del sentiment.

posts.enriched (JSON) -> parsing dei soli campi usati -> event time da `time`
                      -> watermark + deduplica su uri
                      -> finestre tumbling di WINDOW_DURATION (output append: ogni finestra esce una volta, definitiva)
                      -> sentiment.1m (JSON, key = window_start, 1 partizione, finestre in ordine di tempo)
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
INPUT_TOPIC = os.environ["INPUT_TOPIC"]                    # posts.enriched
OUTPUT_TOPIC = os.environ["OUTPUT_TOPIC"]                  # sentiment.1m
STARTING_OFFSETS = os.environ["STARTING_OFFSETS"]
MAX_OFFSETS_PER_TRIGGER = os.environ["MAX_OFFSETS_PER_TRIGGER"]
TRIGGER_INTERVAL = os.environ["TRIGGER_INTERVAL"]
WINDOW_DURATION = os.environ["WINDOW_DURATION"]            # "1 minute"
WATERMARK_DELAY = os.environ["WATERMARK_DELAY"]            # "2 minutes": deve superare il disordine massimo dell'event time
MAX_FUTURE_SKEW = os.environ["MAX_FUTURE_SKEW"]            # "1 minute": tolleranza per `time` nel futuro (DA RIVEDERE)
SHUFFLE_PARTITIONS = os.environ["SHUFFLE_PARTITIONS"]      # fissato nel checkpoint
CHECKPOINT_ROOT = os.environ["CHECKPOINT_ROOT"]

APPROX_DISTINCT_RSD = float(os.getenv("APPROX_DISTINCT_RSD", "0.02"))

# Il checkpoint dipende dal topic di uscita: cambiare finestra significa cambiare
# topic (sentiment.5m, ...) e quindi ripartire con stato nuovo, non riusare quello vecchio.
CHECKPOINT = f"{CHECKPOINT_ROOT}/q2_window/{OUTPUT_TOPIC}"

ISO_UTC = "yyyy-MM-dd'T'HH:mm:ss'Z'"   # la sessione e' in UTC, quindi la 'Z' letterale e' corretta

ENRICHED_SCHEMA = StructType([
    StructField("uri", StringType(), True),
    StructField("did", StringType(), True),
    StructField("time", StringType(), True),
    StructField("sentiment", DoubleType(), True),
    StructField("sentiment_label", StringType(), True),
    StructField("model", StringType(), True),
])

OUTPUT_FIELDS = [
    "window_start", "window_end",
    "n_posts", "n_authors",
    "mean_sentiment", "std_sentiment",
    "share_negative", "share_positive",
    "model",
]


# ---------------------------------------------------------------------------
# Monitoraggio dei dati in ritardo: e' lo strumento per calibrare il watermark.
# Se compaiono righe scartate in funzionamento normale, il watermark e' troppo corto.
# ---------------------------------------------------------------------------
class LateRowsLogger(StreamingQueryListener):
    def onQueryStarted(self, event):
        pass

    def onQueryProgress(self, event):
        p = event.progress
        dropped = sum(op.numRowsDroppedByWatermark for op in p.stateOperators)
        if dropped:
            wm = p.eventTime.get("watermark")
            print(f"[q2_window] batch {p.batchId}: {dropped} righe in ritardo scartate (watermark {wm})")

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        pass


def write_batch_to_kafka(batch_df, batch_id):
    """Scrive le finestre chiuse in questo micro-batch, in ordine di window_start.

    Con il sink Kafka diretto, piu' finestre chiuse nello stesso batch (es. durante un
    recupero di arretrato) verrebbero scritte da task paralleli in ordine arbitrario,
    anche su un topic a 1 partizione. Un solo task che ordina prima di scrivere
    garantisce l'ordine; tra un batch e il successivo l'ordine e' gia' garantito.
    Il volume e' minimo (in regime una finestra per batch), quindi 1 task basta.

    Garanzia at-least-once: se il job cade dopo la scrittura e prima del commit, il batch
    viene rieseguito e le stesse finestre riscritte. Il consumer scarta le finestre con
    window_start <= all'ultima gia' vista.
    """
    (
        batch_df
        .repartition(1)
        .sortWithinPartitions("window_start", "model")
        .select(
            F.col("window_start").alias("key"),
            # null espliciti (es. std_sentiment con un solo post) invece di campi assenti
            F.to_json(F.struct(*OUTPUT_FIELDS), {"ignoreNullFields": "false"}).alias("value"),
        )
        .write.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", OUTPUT_TOPIC)
        .save()
    )


def main():
    spark = (
        SparkSession.builder
        .appName("q2_window")
        .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.streams.addListener(LateRowsLogger())

    # 1) Sorgente Kafka
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", INPUT_TOPIC)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)   # limita i batch al primo avvio (3 giorni di arretrato)
        .option("failOnDataLoss", "false")
        .load()
    )

    # 2) Parsing. Righe incomplete scartate.
    parsed = (
        raw.select(F.from_json(F.col("value").cast("string"), ENRICHED_SCHEMA).alias("p"))
        .select("p.*")
        .withColumn("event_ts", F.try_to_timestamp(F.col("time")))
        .filter(
            F.col("uri").isNotNull()
            & F.col("did").isNotNull()
            & F.col("event_ts").isNotNull()
            & F.col("sentiment").isNotNull()
            & F.col("sentiment_label").isNotNull()
            & F.col("model").isNotNull()
            & (F.col("event_ts") <= F.current_timestamp() + F.expr(f"INTERVAL {MAX_FUTURE_SKEW}"))
        )
    )

    # 3) Watermark + deduplica su uri.
    deduped = (
        parsed
        .withWatermark("event_ts", WATERMARK_DELAY)
        .dropDuplicatesWithinWatermark(["uri"])
    )

    # 4) Finestre tumbling. Raggruppo anche per model: se Q1 cambia modello, i due sentiment
    #    non sono confrontabili e non vanno mescolati nella stessa finestra.
    #    Una finestra senza post non esiste: nessun messaggio. Il detector riconosce i buchi
    #    controllando che window_start sia consecutivo.
    is_neg = (F.col("sentiment_label") == "negative").cast("double")
    is_pos = (F.col("sentiment_label") == "positive").cast("double")

    windowed = (
        deduped
        .groupBy(F.window("event_ts", WINDOW_DURATION).alias("w"), F.col("model"))
        .agg(
            F.count(F.lit(1)).alias("n_posts"),
            F.approx_count_distinct("did", APPROX_DISTINCT_RSD).alias("n_authors"),
            F.avg("sentiment").alias("mean_sentiment"),
            F.stddev_samp("sentiment").alias("std_sentiment"),   # campionaria; null se n_posts = 1
            F.avg(is_neg).alias("share_negative"),
            F.avg(is_pos).alias("share_positive"),
        )
        .select(
            F.date_format(F.col("w.start"), ISO_UTC).alias("window_start"),
            F.date_format(F.col("w.end"), ISO_UTC).alias("window_end"),
            "n_posts", "n_authors",
            "mean_sentiment", "std_sentiment",
            "share_negative", "share_positive",
            "model",
        )
    )

    # 5) Append: una finestra esce solo quando il watermark supera window_end,
    #    cioe' quando si e' visto un post con time >= window_end + WATERMARK_DELAY.
    query = (
        windowed.writeStream
        .queryName("q2_window")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .foreachBatch(write_batch_to_kafka)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()