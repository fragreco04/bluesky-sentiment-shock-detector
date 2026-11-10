# Shock del sentiment su Bluesky

Pipeline in tempo reale che legge i post pubblicati su Bluesky, ne calcola il sentiment e rileva i momenti in cui cambia bruscamente rispetto al suo andamento abituale.

```
Bluesky → producer → Logstash → Kafka → Spark (Q1 sentiment, Q2 finestre, Q3 detector) → Elasticsearch → Kibana
```

I dettagli su componenti, topic e scelte di progetto sono nella cartella della documentazione.

## Requisiti

- Windows con WSL2 e Docker Desktop
- Driver grafico Intel aggiornato: Q1 esegue il modello sulla GPU integrata
- Almeno 8 GB di RAM liberi e una connessione Internet al primo avvio

Per verificare che la GPU sia visibile da WSL:

```bash
ls /dev/dxg
```

Se il file non esiste, Q1 non può partire.

## Primo avvio

**1. Configura l'ambiente.** Copia il file di esempio accanto a `docker-compose.yml`:

```bash
cp example.env .env
```

Se vuoi usare l'explainer, inserisci in `.env` la tua chiave in `GEMINI_API_KEY` (gratuita su [Google AI Studio](https://aistudio.google.com/apikey)). Il file `.env` non va mai messo su git.

**2. Crea il volume dei modelli.** Il compose lo considera esterno e non lo crea da solo:

```bash
docker volume create sentiment-ov
```

In questo modo il modello esportato sopravvive anche a `docker compose down -v`.

**3. Costruisci le immagini.** Richiede qualche minuto, ma si fa una volta sola:

```bash
docker compose build
```

**4. Esporta il modello in OpenVINO.** Q1 usa la versione OpenVINO del modello e senza di essa si riavvia in loop:

```bash
docker compose run --rm --no-deps spark-driver-q1 python3 /opt/spark/jobs/ov_export.py
```

Servono 2-3 GB di RAM liberi.

**5. Avvia la pipeline:**

```bash
docker compose up -d
```

Kafka ed Elasticsearch partono per primi e vengono inizializzati (topic, template e policy); dopo partono Logstash, il producer e i driver Spark. Al primo avvio Q1 compila i kernel della GPU, quindi i primi batch sono più lenti.

**6. Avvia Kibana** quando vuoi vedere i grafici:

```bash
docker compose --profile ui up -d kibana
```

## Verifica

| Cosa | Comando o indirizzo |
|---|---|
| Velocità di Q1 | `docker logs -f spark_q1 2>&1 \| grep progress` |
| Spark UI | `localhost:4040` (Q1), `localhost:4041` (Q2 e Q3) |
| Messaggi su un topic | `docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic sentiment.1m \| jq` |
| Indici Elasticsearch | `docker compose exec -T elasticsearch curl -s "localhost:9200/_cat/indices?v"` |

La prima finestra su `sentiment.1m` arriva dopo circa 3 minuti. Il detector segnala shock solo dopo il warmup di 40 finestre valide, quindi lascia girare la pipeline almeno tre quarti d'ora prima di aspettarti alert.

## Arresto

```bash
docker compose down        # ferma tutto, i dati restano
docker compose down -v     # azzera post, indici e checkpoint (il modello resta)
```