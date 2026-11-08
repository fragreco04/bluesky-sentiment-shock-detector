set -euo pipefail

ES="http://${ES_HOST}:${ES_PORT}"
DIR="$(dirname "$0")"
POSTS_POLICY="posts-retention"      # deve coincidere con index.lifecycle.name in posts.json

# PUT con corpo JSON. --fail-with-body: un errore HTTP fa fallire lo script (e quindi il servizio),
# ma stampa la risposta di Elasticsearch, che dice cosa non va.
put() {
    local out
    if ! out=$(curl -sS --fail-with-body -X PUT "${ES}/$1" -H 'Content-Type: application/json' -d "$2"); then
        echo "ERRORE: PUT /$1" >&2
        echo "${out}" >&2
        exit 22
    fi
    echo "ok: PUT /$1"
}

# Template: il file contiene settings e mappings, il pattern arriva dal .env
put_template() {   # nome, pattern, file
    [ -r "${DIR}/$3" ] || { echo "ERRORE: manca ${DIR}/$3" >&2; exit 1; }
    put "_index_template/$1" "{\"index_patterns\": [\"$2\"], \"priority\": 200, \"template\": $(cat "${DIR}/$3")}"
}

# Crea l'indice solo se manca: HEAD risponde 200 se esiste, 404 se no
create_index() {
    code=$(curl -sS -o /dev/null -w '%{http_code}' -I "${ES}/$1")
    if [ "${code}" = "404" ]; then
        put "$1" '{}'                # il corpo vuoto basta: settings e mappings arrivano dal template
    else
        echo "esiste gia': $1"
    fi
}

# 1) ILM: i post vengono cancellati dopo ES_POSTS_RETENTION_DAYS giorni.
#    L'eta' si conta dalla data nel nome dell'indice (parse_origination_date), non dalla creazione:
#    posts-2026.11.03 viene cancellato il 2026.11.03 + N giorni anche se e' stato creato dopo.
put "_ilm/policy/${POSTS_POLICY}" "{
  \"policy\": {
    \"phases\": {
      \"delete\": { \"min_age\": \"${ES_POSTS_RETENTION_DAYS}d\", \"actions\": { \"delete\": {} } }
    }
  }
}"

# 2) Template
put_template "${ES_INDEX_POSTS}"   "${ES_INDEX_POSTS}-*"  posts.json          # un indice al giorno, creato da Logstash
put_template "${ES_INDEX_MONITOR}" "${ES_INDEX_MONITOR}"  sentiment-1m.json
put_template "${ES_INDEX_ALERTS}"  "${ES_INDEX_ALERTS}"   alerts.json

# 3) Indici fissi: creati subito, cosi' le data view di Kibana funzionano anche prima dei dati.
#    Quelli dei post no: li crea Logstash alla prima scrittura di ogni giorno.
create_index "${ES_INDEX_MONITOR}"
create_index "${ES_INDEX_ALERTS}"

echo "Elasticsearch inizializzato"