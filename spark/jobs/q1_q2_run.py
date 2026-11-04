"""
Q2 + Q3 nella stessa applicazione Spark (un solo driver, una sola JVM).

Le due query restano indipendenti: ognuna ha il suo checkpoint, i suoi topic e il suo
trigger, e girano in thread separati dello stesso driver. Si risparmia una JVM intera
(~0,8 GB) rispetto a due spark-submit, perche' sono entrambe leggere.

Se una delle due termina con un errore, awaitAnyTermination() solleva l'eccezione,
il processo esce e il compose (restart: unless-stopped) riavvia il container:
le query riprendono dall'ultimo batch confermato nei rispettivi checkpoint.
"""
import os

from pyspark.sql import SparkSession

import q2_window
import q3_detect


def main():
    spark = (
        SparkSession.builder
        .appName("q2_q3")
        # Entrambi i job assumono UTC (Q2 formatta le finestre con la 'Z' letterale)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # La funzione detect di Q3 ora vive in un modulo importato: Spark la serializza per
    # riferimento e il worker Python deve poter importare q3_detect. Come sentiment_model in Q1.
    here = os.path.dirname(os.path.abspath(__file__))
    for module in ("q2_window.py", "q3_detect.py"):
        spark.sparkContext.addPyFile(os.path.join(here, module))

    # Ogni start_query imposta le sue shuffle partitions prima di partire
    q2_window.start_query(spark)
    q3_detect.start_query(spark)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()