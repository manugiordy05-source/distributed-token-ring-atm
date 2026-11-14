import socket
import threading
import json
import sys
import time
from datetime import datetime

class ATMNode:
    def __init__(self, node_id, config_path="config.json"):
        self.node_id = node_id
        
        # Caricamento della configurazione di rete
        with open(config_path, "r", encoding="utf-8") as f:
            config = json.load(f)
            
        if node_id not in config["nodes"]:
            raise ValueError(f"ID nodo '{node_id}' non valido. Usa ATM1, ATM2, ATM3 o ATM4.")
            
        self.my_info = config["nodes"][node_id]
        self.successor_id = self.my_info["successor"]
        self.successor_info = config["nodes"][self.successor_id]
        
        self.host = self.my_info["host"]
        self.port = self.my_info["port"]
        self.token_delay = config["token_delay_seconds"]
        self.initial_balance = config["initial_balance"]
        self.has_initial_token = self.my_info["has_initial_token"]
        
        # Memoria privata del nodo: coda delle transazioni locali in attesa
        # Nessuna variabile di saldo globale è condivisa tra processi diversi.
        self.pending_transactions = []
        self.queue_lock = threading.Lock()
        self.running = True

    def log(self, category, message):
        """Stampa log formattati con timestamp per tracciare l'esecuzione."""
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        print(f"[{timestamp}] [{self.node_id}] [{category:<12}] {message}", flush=True)

    # =========================================================================
    # 1. LOGICA DI SCAMBIO MESSAGGI (Rete TCP su localhost)
    # =========================================================================
    def start_server(self):
        """Avvia il server TCP in ascolto dei messaggi dal predecessore nell'anello."""
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((self.host, self.port))
        server_socket.listen(5)
        self.log("SISTEMA", f"In ascolto su {self.host}:{self.port} -> Successore: {self.successor_id}")

        while self.running:
            try:
                conn, _ = server_socket.accept()
                data = conn.recv(4096).decode("utf-8")
                conn.close()
                if data:
                    message = json.loads(data)
                    if message.get("type") == "TOKEN":
                        self.handle_token(message)
            except Exception as e:
                if self.running:
                    self.log("ERRORE", f"Errore ricezione messaggio: {e}")

    def send_token_to_successor(self, token_msg):
        """Inoltra il token esclusivamente al nodo successore nell'anello logico."""
        succ_host = self.successor_info["host"]
        succ_port = self.successor_info["port"]
        
        # Ritardo controllato per rendere leggibile il flusso nei terminali e nel video
        time.sleep(self.token_delay)
        
        while self.running:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((succ_host, succ_port))
                    s.sendall(json.dumps(token_msg).encode("utf-8"))
                self.log("TOKEN_OUT", f"Token inoltrato a {self.successor_id} (Saldo trasportato: {token_msg['balance']:.2f}€)")
                break
            except ConnectionRefusedError:
                self.log("ATTESA", f"In attesa che {self.successor_id} sia avviato sulla porta {succ_port}...")
                time.sleep(2)

    # =========================================================================
    # 2. GESTIONE DEL TOKEN E SEZIONE CRITICA
    # =========================================================================
    def handle_token(self, token_msg):
        """
        Gestisce l'arrivo del Token.
        Il possesso del token garantisce la mutua esclusione: solo questo nodo
        può leggere e modificare il saldo del conto in questo istante.
        """
        current_balance = token_msg["balance"]
        token_seq = token_msg["seq"] + 1
        token_msg["seq"] = token_seq
        
        self.log("TOKEN_IN", f"Token #{token_seq} ricevuto | Saldo attuale: {current_balance:.2f}€")

        # Verifica se il nodo ha una transazione locale in coda da eseguire
        transaction = None
        with self.queue_lock:
            if self.pending_transactions:
                transaction = self.pending_transactions.pop(0)

        if transaction:
            # --- INIZIO SEZIONE CRITICA ---
            new_balance = self.execute_critical_section(current_balance, transaction)
            token_msg["balance"] = new_balance
            # --- FINE SEZIONE CRITICA ---
        else:
            self.log("INFO", "Nessuna transazione in coda. Passaggio rapido del token.")

        # Rilascio della mutua esclusione tramite inoltro al successore
        self.send_token_to_successor(token_msg)

    def execute_critical_section(self, current_balance, tx):
        """
        Esegue in modo atomico i 5 passaggi della transazione bancaria:
        1. Lettura saldo -> 2. Validazione -> 3. Aggiornamento -> 4. Scrittura -> 5. Log
        """
        op_type = tx["op"]
        amount = tx["amount"]
        
        self.log("CS_ENTER", f"=== INIZIO TRANSAZIONE: {op_type} di {amount:.2f}€ ===")
        
        # 1. Lettura del saldo corrente
        self.log("CS_STEP_1", f"Lettura saldo corrente: {current_balance:.2f}€")
        time.sleep(0.4) # Simula tempo di elaborazione atomica
        
        # 2. Validazione dell'operazione
        if op_type == "PRELIEVO" and amount > current_balance:
            self.log("CS_ERRORE", f"Validazione FALLITA: fondi insufficienti ({current_balance:.2f}€ < {amount:.2f}€).")
            self.log("CS_EXIT", "=== FINE TRANSAZIONE (Annullata) ===")
            return current_balance
            
        self.log("CS_STEP_2", "Validazione operazione: OK")
        
        # 3 & 4. Aggiornamento e scrittura del nuovo valore
        if op_type == "DEPOSITO":
            updated_balance = current_balance + amount
        elif op_type == "PRELIEVO":
            updated_balance = current_balance - amount
        else:
            updated_balance = current_balance
            
        self.log("CS_STEP_3_4", f"Scrittura nuovo saldo: {updated_balance:.2f}€")
        
        # 5. Registrazione (log) finale dell'operazione
        self.log("CS_EXIT", f"=== FINE TRANSAZIONE | Saldo aggiornato: {updated_balance:.2f}€ ===")
        return updated_balance

    # =========================================================================
    # 3. INTERFACCIA UTENTE INTERATTIVA (Console del singolo ATM)
    # =========================================================================
    def enqueue_transaction(self, op_type, amount):
        if amount <= 0:
            print("Errore: L'importo deve essere maggiore di zero.")
            return
        with self.queue_lock:
            self.pending_transactions.append({"op": op_type, "amount": amount})
        self.log("CODA_TX", f"Richiesta accodata: {op_type} di {amount:.2f}€ (in attesa del Token)")

    def run(self):
        # Avvio del thread server per ricevere messaggi in background
        server_thread = threading.Thread(target=self.start_server, daemon=True)
        server_thread.start()
        time.sleep(0.5)

        print("\n" + "="*62)
        print(f" TERMINALE {self.node_id} ATTIVO - COMANDI DISPONIBILI:")
        print("   d <importo>   -> Accoda un Deposito (es. d 100)")
        print("   p <importo>   -> Accoda un Prelievo (es. p 200)")
        print("   scenario      -> Carica la transazione dell'esempio di progetto")
        if self.has_initial_token:
            print("   start         -> AVVIA IL TOKEN RING (Solo su ATM1)")
        print("   exit          -> Chiude il nodo")
        print("="*62 + "\n")

        # Loop di lettura comandi da tastiera
        while self.running:
            try:
                cmd = input().strip().lower()
                if not cmd:
                    continue
                parts = cmd.split()
                
                if parts[0] == "start" and self.has_initial_token:
                    initial_token = {
                        "type": "TOKEN",
                        "balance": self.initial_balance,
                        "seq": 0
                    }
                    self.log("AVVIO", f"Generazione Token iniziale con saldo {self.initial_balance:.2f}€")
                    threading.Thread(target=self.send_token_to_successor, args=(initial_token,), daemon=True).start()
                    
                elif parts[0] == "d" and len(parts) == 2:
                    self.enqueue_transaction("DEPOSITO", float(parts[1]))
                    
                elif parts[0] == "p" and len(parts) == 2:
                    self.enqueue_transaction("PRELIEVO", float(parts[1]))
                    
                elif parts[0] == "scenario":
                    # Carica esattamente lo scenario descritto nella Sezione 7 della traccia
                    if self.node_id == "ATM1":
                        self.log("SCENARIO", "ATM1 non ha transazioni nello scenario base.")
                    elif self.node_id == "ATM2":
                        self.enqueue_transaction("PRELIEVO", 200.0)
                    elif self.node_id == "ATM3":
                        self.enqueue_transaction("DEPOSITO", 100.0)
                    elif self.node_id == "ATM4":
                        self.enqueue_transaction("PRELIEVO", 500.0)
                        
                elif parts[0] == "exit":
                    self.running = False
                    print(f"Chiusura {self.node_id}...")
                    sys.exit(0)
                else:
                    print("Comando non riconosciuto. Usa: d <num>, p <num>, scenario, start (su ATM1), exit.")
            except ValueError:
                print("Errore: Inserisci un valore numerico valido.")
            except KeyboardInterrupt:
                self.running = False
                sys.exit(0)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso corretto: python atm_node.py <ATM1|ATM2|ATM3|ATM4>")
        sys.exit(1)
    
    node = ATMNode(sys.argv[1].upper())
    node.run()