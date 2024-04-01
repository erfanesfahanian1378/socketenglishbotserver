const express = require('express');
const http = require('http');
const socketIo = require('socket.io');

// Create a new express application
const app = express();
let waitingUsers = [];

// Create an HTTP server and wrap the Express app
const server = http.createServer(app);

// Attach socket.io to the server
const io = socketIo(server);

const redis = require('redis');

const client = redis.createClient({
    url: 'redis://localhost:6379' // Default host and port
});
let idChatToSocketMap = {};
let socketIdToIdChatMap = {};

client.connect();
client.on('error', (err) => console.log('Redis Client Error', err));

// Serve static files from the public directory (optional)
app.use(express.static('public'));

// Handle WebSocket connections
io.on('connection', (socket) => {
    console.log('A user connected');
    socket.on('requestChat', async (data) => {

        console.log("this is the data from all queue");
        try {
            const queue = await client.lRange("waitingUsers", 0, -1); // Retrieve the entire queue
            const deserializedQueue = queue.map(item => JSON.parse(item)); // Deserialize each item
            console.log(deserializedQueue); // Send the deserialized queue as JSON
        } catch (error) {
            console.error('Error retrieving queue:', error);
        }
        console.log("this is the data from all queue");


        if (data) {
            idChatToSocketMap[data.idchat] = socket;
            socketIdToIdChatMap[socket.id] = data.idchat;
            const waitingCount = await client.lLen('waitingUsers');
            if (waitingCount > 0) {
                console.log("there is a user in the pop");
                // Pop the first waiting user's idChat object from the queue
                const partnerIdChat = await client.lPop('waitingUsers');
                console.log(partnerIdChat);
                console.log("there is a user in the pop");
                // Retrieve sockets using idChat from your method of mapping
                const partnerSocket = idChatToSocketMap[JSON.parse(partnerIdChat)]; /* retrieve socket using JSON.parse(partnerIdChat).idchat */
                const requesterSocket = idChatToSocketMap[data.idchat];


                // Notify both users of the match
                requesterSocket.emit('matchFound', JSON.parse(partnerIdChat));
                partnerSocket.emit('matchFound', data.idchat);

                // Logic to create a room or direct channel for communication
            } else {
                console.log("there is no one in the queue add this one");
                // No match found, add user's idChat to the waiting queue
                await addToQueueIfNotExists(data);
                // await client.rPush('waitingUsers', JSON.stringify(data.idchat));
            }
        }
        const waitingCount2 = await client.lLen('waitingUsers');
        console.log(waitingCount2);
        console.log(client);
    });

    socket.on('disconnect', async () => {
        console.log("it is in the disconnect");
        const idChatId = socketIdToIdChatMap[socket.id];
        if (idChatId) {
            const queueKey = 'waitingUsers';
            // Serialize the identifier the same way as when you added it
            const idChatStringToRemove = JSON.stringify(idChatId);
            await client.lRem(queueKey, 1, idChatStringToRemove);
            console.log(`Removed user ${idChatId} from queue`);

            // Clean up both mappings
            delete idChatToSocketMap[idChatId];
            delete socketIdToIdChatMap[socket.id];
        }
    });
});



async function addToQueueIfNotExists(idChat) {
    const queueKey = 'waitingUsers';
    const idChatString = JSON.stringify(idChat.idchat);

    // Retrieve the entire queue (consider efficiency here)
    const queue = await client.lRange(queueKey, 0, -1);

    // Check if idChat.idchat exists in the queue
    const exists = queue.some((item) => {
        const itemData = JSON.parse(item);
        return itemData === idChat.idchat;
    });

    if (!exists) {
        // Add to the queue if not exists
        await client.rPush(queueKey, idChatString);
        console.log('User added to queue:', idChat.idchat);
    } else {
        console.log('User already in queue:', idChat.idchat);
    }
}

const PORT = process.env.PORT || 3002;
server.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});
