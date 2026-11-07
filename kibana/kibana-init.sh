#!/bin/bash
# Inizializzazione di Kibana: importa data view, visualizzazioni e dashboard da dashboards.ndjson.
#
# Come es-init per Elasticsearch: gira a ogni avvio del profilo "ui" ed e' idempotente.
# overwrite=true: gli oggetti con lo stesso id vengono sostituiti dalla versione del file.
# Se modifichi una dashboard in Kibana, salvala come copia ("Save as new"): l'originale
# verrebbe riportato a questa versione al prossimo avvio.
set -euo pipefail

KIBANA="${KIBANA_URL}"
FILE="$(dirname "$0")/dashboards.ndjson"
[ -r "${FILE}" ] || { echo "ERRORE: manca ${FILE}" >&2; exit 1; }

# Kibana impiega un po' ad avviarsi: aspetta che l'API risponda (al massimo 5 minuti)
echo "Attendo Kibana su ${KIBANA} ..."
for _ in $(seq 1 60); do
    code=$(curl -s -o /dev/null -w '%{http_code}' "${KIBANA}/api/status" || true)
    [ "${code}" = "200" ] && break
    sleep 5
done
[ "${code}" = "200" ] || { echo "ERRORE: Kibana non risponde (ultimo codice HTTP: ${code})" >&2; exit 1; }

# Import. kbn-xsrf e' obbligatorio per le chiamate che modificano qualcosa
out=$(curl -sS -X POST "${KIBANA}/api/saved_objects/_import?overwrite=true" \
      -H 'kbn-xsrf: true' --form file=@"${FILE}")
if ! echo "${out}" | grep -q '"success":true'; then
    echo "ERRORE: import fallito. Risposta di Kibana:" >&2
    echo "${out}" >&2
    exit 1
fi
echo "ok: $(echo "${out}" | grep -o '"successCount":[0-9]*') oggetti importati"

# Data view predefinita: quella che Discover apre per prima
curl -sS -o /dev/null -X POST "${KIBANA}/api/data_views/default" \
     -H 'kbn-xsrf: true' -H 'Content-Type: application/json' \
     -d '{"data_view_id": "dv-sentiment-1m", "force": true}' || true

echo "Kibana inizializzato: http://localhost:5601/app/dashboards"