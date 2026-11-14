# Sistema di Transazioni Bancarie Distribuite con Mutua Esclusione a Token Ring

Implementazione di un sistema distribuito composto da **4 nodi indipendenti (ATM1, ATM2, ATM3, ATM4)** su `localhost` che coordinano l'accesso a un conto bancario condiviso tramite l'algoritmo **Token Ring** e scambio di messaggi TCP.

## Architettura
* **Topologia Logica:** `ATM1 -> ATM2 -> ATM3 -> ATM4 -> ATM1`
* **Assenza di Memoria Condivisa:** Ogni nodo è un processo isolato. Lo stato del saldo viene incapsulato e trasferito esclusivamente all'interno del messaggio `TOKEN`.
* **Mutua Esclusione:** Solo il nodo che detiene il token può entrare nella sezione critica ed eseguire atomicamente i 5 passaggi di una transazione (Lettura, Validazione, Aggiornamento, Scrittura, Logging).

## Requisiti
* **Linguaggio:** Python 3.8+
* **Dipendenze:** Nessuna libreria esterna richiesta (utilizza esclusivamente la Standard Library: `socket`, `threading`, `json`, `sys`, `time`, `datetime`).

## Esecuzione
Aprire **4 terminali separati** nella cartella del progetto ed eseguire:
1. **Terminale 1:** `python atm_node.py ATM1`
2. **Terminale 2:** `python atm_node.py ATM2`
3. **Terminale 3:** `python atm_node.py ATM3`
4. **Terminale 4:** `python atm_node.py ATM4`

### Comandi Disponibili nei Terminali
* `d <importo>` : Accoda un deposito (es. `d 100`)
* `p <importo>` : Accoda un prelievo (es. `p 200`)
* `scenario` : Carica la transazione dell'esempio di riferimento della traccia
* `start` : Avvia la circolazione del token (da eseguire solo su `ATM1`)
* `exit` : Chiude il nodo