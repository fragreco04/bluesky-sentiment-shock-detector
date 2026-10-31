import asyncio
import json
import os

import websockets

JETSTREAM_URL = os.getenv("JETSTREAM_URL", "wss://default.no")

LOGSTASH_HOST = os.getenv("LOGSTASH_HOST", "logstash")
LOGSTASH_PORT = int(os.getenv("LOGSTASH_PORT", "5000"))

async def main():
    # Ci connettiamo a logstash con una connessione TCP
    _, writer = await asyncio.open_connection(LOGSTASH_HOST, LOGSTASH_PORT)

    # Ci connettiamo al jetsream usando la websocket
    async with websockets.connect(JETSTREAM_URL) as websocket:
        print(f"Connesso a Jetstream: {JETSTREAM_URL}")
        print(f"Invio eventi a Logstash: {LOGSTASH_HOST}:{LOGSTASH_PORT}")

        async for message in websocket:
            # Preleviamo il contenuto dalla websocket, jetstream invia i dati in singoli json
            event = json.loads(message)

            payload = json.dumps(event, ensure_ascii=False) + "\n"   # Aggiungiamo un a capo, in modo che logstash li legge come singoli eventi
            writer.write(payload.encode("utf-8"))                   # Converte il messaggio in byte e li invia usando TCP, non aspettando che il destinatario li abbia ricevuti
            await writer.drain()                                    # Sospende la coroutine se il buffer di invio è troppo pieno

if __name__ == "__main__":
    asyncio.run(main())