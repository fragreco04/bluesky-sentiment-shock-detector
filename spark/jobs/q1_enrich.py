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
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

import sentiment_model   # sta accanto a questo file; all'import non carica torch ne' il modello

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
OV_DIR = os.environ["SENTIMENT_OV_DIR"]                          # modelli OpenVINO esportati da ov_export.py

# Tuning dell'inferenza: dal .env, come gli altri parametri (se manca, il job non parte)
INFER_BATCH_SIZE = int(os.environ["INFER_BATCH_SIZE"])           # testi per passata del modello
INFER_MAX_LENGTH = int(os.environ["INFER_MAX_LENGTH"])           # token massimi per testo
INFER_THREADS = int(os.environ["INFER_THREADS"])                 # thread di inferenza per worker Python
# Motore: torch (PyTorch), ov:fp32 (OpenVINO su CPU), ov:fp32@GPU (vedi sentiment_model).
# Un valore sconosciuto, o un modello OpenVINO non esportato, ferma il job subito, sul driver.
INFER_ENGINE = sentiment_model.check_engine(os.environ["INFER_ENGINE"], OV_DIR)

# Il campo `model` dice con cosa e' stato calcolato il sentiment. Ogni motore diverso da
# PyTorch da' valori (di poco) diversi: con un'etichetta propria (es. "+ov:fp32@GPU") Q2 non lo
# mescola agli altri nella stessa finestra e Q3 gli costruisce una baseline separata.
MODEL_LABEL = MODEL_NAME if INFER_ENGINE == "torch" else f"{MODEL_NAME}+{INFER_ENGINE}"

CHECKPOINT = f"{CHECKPOINT_ROOT}/q1_enrich"

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
    # Timestamp dell'evento da `time`, stringa ISO 8601 in UTC (es. "2026-08-13T06:47:43.959305Z").
    #
    # Utilizzando try_to_timestamp(), spark assegna null al valore contenuto in 'time' in caso la stringa non è leggibile
    # Altrimenti, di default, Spark lancerebbe un errore
    
    return F.try_to_timestamp(F.col("time"))


# Factory: restituisce la UDF pronta all'uso, restituisce i valori che ci interessano nell'arricchimento
def build_sentiment_udf():
    @F.pandas_udf(SENTIMENT_SCHEMA)             # Decoratore che trasforma una funzione Python in una pandas UDF
    def sentiment_udf(batches: Iterator[pd.Series]) -> Iterator[pd.DataFrame]:          # Riceve blocchi di righe
        # importiamo sull'executor.
        import sentiment_model

        for texts in batches:
            scores, labels = sentiment_model.score(
                texts.fillna("").tolist(),
                MODEL_PATH,
                batch_size=INFER_BATCH_SIZE,
                max_length=INFER_MAX_LENGTH,
                num_threads=INFER_THREADS,
                engine=INFER_ENGINE,
                ov_dir=OV_DIR,
            )
            yield pd.DataFrame({"sentiment": scores, "sentiment_label": labels})

    # Non deterministica: l'ottimizzatore non puo' duplicarla. Senza, quando la struct
    # restituita viene letta in due colonne (s.sentiment e s.sentiment_label), Spark
    # potrebbe copiare la chiamata in entrambe ed eseguire l'inferenza due volte.
    # Il risultato non cambia: cambia solo cio' che l'ottimizzatore puo' fare.
    return sentiment_udf.asNondeterministic()


class ProgressLogger(StreamingQueryListener):
    # Una riga di log per micro-batch: quanti post, a che velocita' arrivano e a che velocita'
    # vengono elaborati. Con batch pieni (righe = MAX_OFFSETS_PER_TRIGGER), "elab" e' la capacita'
    # reale di Q1 dentro Spark; addBatch e' in pratica il tempo dell'inferenza.
    #   docker logs -f spark_q1 2>&1 | grep progress
    def onQueryStarted(self, event):
        pass

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        pass

    def onQueryProgress(self, event):
        p = event.progress
        d = p.durationMs or {}
        print(f"[progress] batch={p.batchId} righe={p.numInputRows} "
              f"in={p.inputRowsPerSecond:.1f}/s elab={p.processedRowsPerSecond:.1f}/s "
              f"addBatch={d.get('addBatch', 0)}ms totale={d.get('triggerExecution', 0)}ms",
              flush=True)


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
    spark.streams.addListener(ProgressLogger())
    print(f"[q1] motore {INFER_ENGINE}, campo model = {MODEL_LABEL}", flush=True)

    # Rende sentiment_model.py importabile dai worker Python. Viaggia solo il modulo:
    # il modello lo carica ogni worker dal disco dell'immagine (SENTIMENT_MODEL_PATH)
    here = os.path.dirname(os.path.abspath(__file__))
    spark.sparkContext.addPyFile(os.path.join(here, "sentiment_model.py"))

    # Chiama la factory e ottiene l'oggetto UDF
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

    # 2) Parsing dei messaggi
    parsed = (
        raw.select(F.from_json(F.col("value").cast("string"), RAW_SCHEMA).alias("p"))
        .select("p.*")
        .withColumn("event_ts", event_time_col())
        .filter(                                    # Scartiamo le righe con i campi null
            F.col("uri").isNotNull()
            & F.col("did").isNotNull()
            & F.col("event_ts").isNotNull()
        )
    )

    # 3) Deduplica su uri
    deduped = (
        parsed
        .withWatermark("event_ts", DEDUP_WATERMARK)
        .dropDuplicatesWithinWatermark(["uri"])
    )

    # 4) Arricchimento
    enriched = (
        deduped
        .withColumn("s", sentiment_udf(F.col("text")))          # Calcolo colonna "s" che contiene (sentiment, sentiment_label)
        .select(
            "uri", "did", "time", "text",
            "link_title", "reply_parent_uri", "quoted_uri",
            F.col("s.sentiment").alias("sentiment"),
            F.col("s.sentiment_label").alias("sentiment_label"),
            F.lit(MODEL_LABEL).alias("model"),
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