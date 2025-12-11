const { Kafka } = require('kafkajs');
const { Server } = require("socket.io");

// Broker configuration: use the environment variable (Docker) or localhost (fallback)
const KAFKA_BROKER = process.env.KAFKA_BROKER || 'localhost:29092';
const TOPIC = 'alerts';

// Setup WebSocket Server on port 3000
const io = new Server(3000, {
    cors: { origin: "*" }
});

console.log("📡 [WS GATEWAY] WebSocket Server started on port 3000");

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

// Connection function with automatic retry
const connectWithRetry = async () => {
    try {
        console.log(`⏳ [KAFKA] Attempting to connect to ${KAFKA_BROKER}...`);
        
        await consumer.connect();
        console.log("✅ [KAFKA] Successfully connected!");
        
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
        console.error(`❌ [KAFKA ERROR] Unable to connect: ${error.message}`);
        console.log("🔄 Trying again in 5 seconds...");
        setTimeout(connectWithRetry, 5000);
    }
};

// Start
connectWithRetry();