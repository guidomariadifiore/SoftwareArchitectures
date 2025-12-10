Dimostrazione del traffico:

Assicurati che nel file command.txt ci sia scritto NORMAL.
Avvia lo script. Vedrai log tipo: 📡 [LIVE] Inviato: 52.4 km/h.
Apri command.txt, scrivi CRASH e salva.
Nel terminale vedrai la velocità crollare: 📡 [LIVE] Inviato: 0.0 km/h | Status: BLOCKED.
(Più avanti) Questo attiverà l'alert sulla tua dashboard.

Dimostrazione della resilience:
Mentre lo script gira ed è connesso ([LIVE]), spegni il broker Mosquitto.
Osserva il terminale, dove ci sono i messaggi che indicano che il sistema sta salvando i dati nel buffer
Riaccendi Mosquitto (o ripristina la rete).
Osserva il terminale, dove ci sono i messaggi che indicano che i messaggi nel buffer si stanno reinviando
