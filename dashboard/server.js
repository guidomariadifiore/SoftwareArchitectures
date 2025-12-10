const { Kafka } = require('kafkajs');
const { Server } = require("socket.io");

// Configurazione Broker: usa la variabile d'ambiente (Docker) o localhost (fallback)
const KAFKA_BROKER = process.env.KAFKA_BROKER || 'localhost:29092';
const TOPIC = 'alerts';

// Setup WebSocket Server sulla porta 3000
const io = new Server(3000, {
    cors: { origin: "*" }
});

console.log("📡 [WS GATEWAY] WebSocket Server avviato su porta 3000");

// Setup Kafka Client
const kafka = new Kafka({
    clientId: 'dashboard-backend',
    brokers: [KAFKA_BROKER],
    retry: {
        initialRetryTime: 300,
        retries: 10
    }
});

const consumer = kafka.consumer({ groupId: 'dashboard-group-1' });

// Funzione di connessione con Retry automatico
const connectWithRetry = async () => {
    try {
        console.log(`⏳ [KAFKA] Tentativo di connessione a ${KAFKA_BROKER}...`);
        
        await consumer.connect();
        console.log("✅ [KAFKA] Connesso con successo!");
        
        await consumer.subscribe({ topic: TOPIC, fromBeginning: false });

        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                const alertString = message.value.toString();
                console.log(`📨 [ALERT] ${alertString}`);
                // Invia al browser
                io.emit('alert', JSON.parse(alertString));
            },
        });
    } catch (error) {
        console.error(`❌ [KAFKA ERROR] Impossibile connettersi: ${error.message}`);
        console.log("🔄 Riprovo tra 5 secondi...");
        setTimeout(connectWithRetry, 5000);
    }
};

// Avvio
connectWithRetry();