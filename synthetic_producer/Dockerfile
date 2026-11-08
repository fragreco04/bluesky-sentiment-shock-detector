FROM python:3.12-slim

WORKDIR /app

# Solo libreria standard (tomllib e' incluso da Python 3.11): niente requirements.txt
COPY synthetic_producer.py textgen.py ./

# Copia di riserva dello scenario: si usa se la cartella config non viene montata.
# Il compose monta ./synthetic_producer/config su /config e punta SCENARIO_FILE li'.
COPY config/scenario.toml /app/default-scenario.toml
ENV SCENARIO_FILE=/app/default-scenario.toml

CMD ["python", "-u", "synthetic_producer.py"]