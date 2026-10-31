"""
Q1 - Arricchimento dei post Bluesky.

posts.raw (JSON) -> parsing con schema esplicito -> deduplica su uri
                 -> sentiment (cardiffnlp/twitter-roberta-base-sentiment-latest)
                 -> posts.enriched (JSON, key = uri)
"""
import os
from typing import Iterator

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

# ---------------------------------------------------------------------------
# Configurazione
# Viene letta sul DRIVER. I valori usati dentro la UDF viaggiano serializzati
# insieme alla UDF: gli executor non vedono le variabili d'ambiente del driver.
#
# os.environ[...] (senza default) per tutto cio' che arriva dal .env tramite il
# compose: c'e' una sola fonte del valore e, se manca, il job si ferma subito con
# un KeyError invece di usare un valore che nessuno ha scelto.
# os.getenv(..., default) solo per i parametri di tuning definiti esclusivamente qui.
# ---------------------------------------------------------------------------
# Dal .env, tramite il compose
KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
INPUT_TOPIC = os.environ["INPUT_TOPIC"]
OUTPUT_TOPIC = os.environ["OUTPUT_TOPIC"]
STARTING_OFFSETS = os.environ["STARTING_OFFSETS"]
MAX_OFFSETS_PER_TRIGGER = os.environ["MAX_OFFSETS_PER_TRIGGER"]  # tetto al micro-batch: l'inferenza e' lenta
TRIGGER_INTERVAL = os.environ["TRIGGER_INTERVAL"]
DEDUP_WATERMARK = os.environ["DEDUP_WATERMARK"]                  # per quanto ricordo un uri gia' visto
SHUFFLE_PARTITIONS = os.environ["SHUFFLE_PARTITIONS"]            # = core totali; viene fissato nel checkpoint
CHECKPOINT_ROOT = os.environ["CHECKPOINT_ROOT"]
MODEL_NAME = os.environ["SENTIMENT_MODEL_NAME"]
MODEL_PATH = os.environ["SENTIMENT_MODEL_PATH"]

# Tuning dell'inferenza: definiti solo qui
INFER_BATCH_SIZE = int(os.getenv("INFER_BATCH_SIZE", "32"))
INFER_MAX_LENGTH = int(os.getenv("INFER_MAX_LENGTH", "256"))
TORCH_THREADS = int(os.getenv("TORCH_THREADS", "1"))             # thread torch per worker Python

CHECKPOINT = f"{CHECKPOINT_ROOT}/q1_enrich"

# ---------------------------------------------------------------------------
# Schemi
# ---------------------------------------------------------------------------
# `langs` e' nel messaggio ma non lo leggiamo: from_json ignora i campi non dichiarati.
RAW_SCHEMA = StructType([
    StructField("uri", StringType(), True),
    StructField("did", StringType(), True),
    # ISO 8601 UTC: resta stringa per uscire invariato; l'event time si ricava a parte
    StructField("time", StringType(), True),
    StructField("text", StringType(), True),
    StructField("link_title", StringType(), True),
    StructField("reply_parent_uri", StringType(), True),
    StructField("quoted_uri", StringType(), True),
])

# nullable=True obbligatorio: Arrow restituisce sempre campi nullable e Spark 4 confronta
# lo schema dichiarato con quello ricevuto, nullabilita' compresa. Con False ogni task fallisce
# ("Invalid schema from arrow-enabled Python UDTF"). La UDF non produce comunque null.
SENTIMENT_SCHEMA = StructType([
    StructField("sentiment", DoubleType(), True),
    StructField("sentiment_label", StringType(), True),
])

OUTPUT_FIELDS = [
    "uri", "did", "time", "text",
    "link_title", "reply_parent_uri", "quoted_uri",
    "sentiment", "sentiment_label", "model",
]


def event_time_col():
    """Timestamp dell'evento da `time`, stringa ISO 8601 in UTC (es. "2026-08-13T06:47:43.959305Z").

    Niente pattern esplicito: il parser di default di Spark gestisce la 'T', la 'Z' e da 0 a 6
    cifre di frazione. Un pattern fisso con .SSSSSS fallirebbe se il producer omettesse la
    frazione quando i microsecondi sono zero (come fa datetime.isoformat() di Python).
    try_to_timestamp restituisce null invece di lanciare un errore (ANSI attivo di default in Spark 4):
    le righe con `time` illeggibile vengono poi scartate dal filtro.
    """
    return F.try_to_timestamp(F.col("time"))


def build_sentiment_udf():
    @F.pandas_udf(SENTIMENT_SCHEMA)
    def sentiment_udf(batches: Iterator[pd.Series]) -> Iterator[pd.DataFrame]:
        import sentiment_model  # gira sull'executor; il file arriva con addPyFile

        for texts in batches:
            scores, labels = sentiment_model.score(
                texts.fillna("").tolist(),
                MODEL_PATH,
                batch_size=INFER_BATCH_SIZE,
                max_length=INFER_MAX_LENGTH,
                num_threads=TORCH_THREADS,
            )
            yield pd.DataFrame({"sentiment": scores, "sentiment_label": labels})

    return sentiment_udf


def main():
    spark = (
        SparkSession.builder
        .appName("q1_enrich")
        # Default 200: troppe per uno stato piccolo. Cambiarlo dopo il primo avvio
        # non ha effetto finche' esiste il checkpoint.
        .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
        # I timestamp sono in UTC: cosi' anche finestre e log li mostrano in UTC
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Distribuisce il modulo del modello a tutti gli executor
    here = os.path.dirname(os.path.abspath(__file__))
    spark.sparkContext.addPyFile(os.path.join(here, "sentiment_model.py"))

    sentiment_udf = build_sentiment_udf()

    # 1) Sorgente Kafka
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", INPUT_TOPIC)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .option("failOnDataLoss", "false")      # la retention puo' cancellare offset non ancora letti
        .load()
    )

    # 2) Parsing: i messaggi illeggibili diventano righe con campi null e vengono scartati
    parsed = (
        raw.select(F.from_json(F.col("value").cast("string"), RAW_SCHEMA).alias("p"))
        .select("p.*")
        .withColumn("event_ts", event_time_col())
        .filter(
            F.col("uri").isNotNull()
            & F.col("did").isNotNull()
            & F.col("event_ts").isNotNull()
        )
    )

    # 3) Deduplica su uri. Il watermark limita lo stato: un uri viene ricordato per
    #    DEDUP_WATERMARK; un duplicato che arriva dopo non viene piu' riconosciuto.
    deduped = (
        parsed
        .withWatermark("event_ts", DEDUP_WATERMARK)
        .dropDuplicatesWithinWatermark(["uri"])
    )

    # 4) Arricchimento: dopo la deduplica, cosi' non si fa inferenza due volte sullo stesso post
    enriched = (
        deduped
        .withColumn("s", sentiment_udf(F.col("text")))
        .select(
            "uri", "did", "time", "text",
            "link_title", "reply_parent_uri", "quoted_uri",
            F.col("s.sentiment").alias("sentiment"),
            F.col("s.sentiment_label").alias("sentiment_label"),
            F.lit(MODEL_NAME).alias("model"),
        )
    )

    # 5) Scrittura su Kafka: key = uri (stesso post -> stessa partizione), value = JSON.
    #    I campi opzionali null vengono omessi dal JSON.
    out = enriched.select(
        F.col("uri").alias("key"),
        F.to_json(F.struct(*OUTPUT_FIELDS)).alias("value"),
    )

    query = (
        out.writeStream.format("kafka")
        .queryName("q1_enrich")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", OUTPUT_TOPIC)
        .option("checkpointLocation", CHECKPOINT)
        .outputMode("append")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()