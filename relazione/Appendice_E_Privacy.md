# Appendice E – Privacy e GDPR

Questa appendice approfondisce gli aspetti di privacy accennati nei capitoli 2, 7 e 11 della [relazione](Relazione.md).

## E.1 Le misure adottate

Anche se i post sono pubblici, contengono testi personali e sono associati a un identificativo dell'autore, quindi rientrano tra i dati personali tutelati dal **GDPR**. La pipeline applica due dei suoi principi fondamentali, la minimizzazione dei dati e la limitazione della conservazione: tiene solo i campi necessari, scarta i post nelle altre lingue il prima possibile e cancella i singoli post dopo tre giorni. Restano senza scadenza solo i dati aggregati, cioè le statistiche minuto per minuto e gli alert, che non contengono testi né riferimenti ai singoli utenti.

Queste scelte sono un buon punto di partenza, ma non bastano da sole a rendere il sistema conforme. Un uso in un contesto aziendale richiederebbe una verifica più completa, per esempio sulla base giuridica del trattamento e sulla gestione degli account cancellati, che la pipeline attualmente non riceve da Jetstream.

## E.2 La gestione delle cancellazioni

Il punto più concreto da affrontare è la gestione delle cancellazioni.

**Il problema.** Il GDPR riconosce a ogni persona il diritto di far cancellare i propri dati. Su Bluesky un utente può cancellare un singolo post o l'intero account, ma oggi la pipeline non se ne accorge. Logstash scarta di proposito le cancellazioni dei post (sezione 3.3 della relazione), e gli eventi che segnalano la cancellazione di un account non arrivano proprio, perché la connessione a Jetstream chiede solo gli eventi sui post. Un post cancellato dal suo autore resta quindi nella pipeline fino a tre giorni, in Kafka e in Elasticsearch.

C'è anche un secondo aspetto, meno evidente. I dati conservati senza scadenza non sono tutti anonimi. La serie al minuto contiene solo statistiche. I documenti degli alert, e il topic `alerts.explained` da cui provengono, contengono invece gli `uri` dei post usati dall'explainer, e ogni `uri` include l'identificativo del suo autore. Contengono anche i titoli dei link e la spiegazione del modello, che può citare dei post. Sono pochi dati, alcune decine di riferimenti per episodio, ma sono riferimenti a persone, conservati per sempre.

**Le soluzioni.** Le soluzioni possibili hanno difficoltà diverse:

- **Ricevere le cancellazioni.** Jetstream può inviare anche gli eventi `account`, che segnalano un account cancellato, disattivato o sospeso. Basterebbe aggiungerli alla connessione del producer e smettere di scartare le cancellazioni dei post in Logstash, indirizzandole verso un topic dedicato.
- **Cancellare da Elasticsearch.** Un piccolo servizio, simile all'explainer, leggerebbe quel topic e cancellerebbe da Elasticsearch i post interessati: il singolo post, usando il suo `uri`, oppure tutti i post di un account, usando il suo identificativo. Elasticsearch permette entrambe le operazioni.
- **Ripulire gli alert.** Lo stesso servizio dovrebbe togliere dai documenti degli alert i riferimenti a quell'account. In alternativa si potrebbe cancellare dagli alert l'elenco degli `uri` dopo tre giorni, come per i post, conservando solo la spiegazione e i termini significativi.
- **Kafka.** È la parte più difficile, perché Kafka non permette di cancellare un singolo messaggio da un topic. Per i topic dei post la conservazione di tre giorni limita già il problema. Per gli alert, l'alternativa più semplice è non salvare affatto gli `uri` in Kafka e lasciarli solo in Elasticsearch, dove si possono cancellare.

Oltre alle cancellazioni, un uso reale richiederebbe una verifica legale più ampia, che va oltre gli scopi di questo progetto. Andrebbero individuate la **base giuridica** del trattamento, per esempio il legittimo interesse, e le informazioni da dare agli interessati. Andrebbe valutato il trasferimento dei testi a un fornitore esterno attraverso l'explainer (capitolo 7 e Appendice B), e probabilmente andrebbe fatta una valutazione d'impatto, prevista dal GDPR per i trattamenti che osservano in modo sistematico grandi quantità di persone.

