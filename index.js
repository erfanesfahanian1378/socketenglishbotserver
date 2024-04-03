const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const mongoose = require('mongoose');

// Create a new express application
const app = express();
let waitingUsers = [];

// Create an HTTP server and wrap the Express app
const server = http.createServer(app);

// Attach socket.io to the server
const io = socketIo(server);
mongoose.connect('mongodb://localhost/testmongo', {useNewUrlParser: true, useUnifiedTopology: true})
    .then(() => console.log('Connected to MongoDB...'))
    .catch(err => console.error('Could not connect to MongoDB...', err));

const redis = require('redis');


const chatPartnerSchema = new mongoose.Schema({
    firstUserId: String,
    secondUserId: String,
    whenTheyEnd: Date
});

const ChatPartner = mongoose.model('ChatPartner', chatPartnerSchema);


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
    socket.on('requestChat', async (data) => {

        console.log("+++++++++++++++who is actively chatting with who+++++++++++++++++++++++++++");
        await fetchActiveChatSessions()
        console.log("+++++++++++++++who is actively chatting with who+++++++++++++++++++++++++++");

        // console.log("this is the data from all queue");
        // try {
        //     const queue = await client.lRange("waitingUsers", 0, -1); // Retrieve the entire queue
        //     const deserializedQueue = queue.map(item => JSON.parse(item)); // Deserialize each item
        //     console.log(deserializedQueue); // Send the deserialized queue as JSON
        // } catch (error) {
        //     console.error('Error retrieving queue:', error);
        // }
        // console.log("this is the data from all queue");


        if (data) {
            idChatToSocketMap[data.idchat] = socket;
            socketIdToIdChatMap[socket.id] = data.idchat;
            const waitingCount = await client.lLen('waitingUsers');
            if (waitingCount > 0) {
                console.log("there is a user in the pop-------------");
                // Pop the first waiting user's idChat object from the queue
                const partnerIdChat = await client.lPop('waitingUsers');

                //need to check them for the connecting and logging
                const requesterIdChat = data.idchat;

                const sessionKeyUser1 = `chatSession:${partnerIdChat}`;
                const sessionKeyUser2 = `chatSession:${requesterIdChat}`;
                await client.set(sessionKeyUser1, requesterIdChat);
                await client.set(sessionKeyUser2, JSON.parse(partnerIdChat));
                //need to check them for the connecting and logging

                // this await part is for that i can see which user is talking to which
                await fetchActiveChatSessions()
                // this await part is for that i can see which user is talking to which

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


            //delete the session for that they chat with each other

            const sessionKey = `chatSession:${idChatId}`;
            console.log("this is the session key");
            console.log(sessionKey);
            console.log("this is the session key");
            const partnerIdChat = await client.get(sessionKey);

            if (partnerIdChat) {
                // Notify the chat partner if necessary
                const partnerSocket = idChatToSocketMap[partnerIdChat];
                if (partnerSocket) {
                    partnerSocket.emit('chatPartnerDisconnected', { message: 'Your chat partner has disconnected.' });
                }

                // Delete both users' chat session keys from Redis
                await client.del(sessionKey);
                const partnerSessionKey = `chatSession:${partnerIdChat}`;
                await client.del(partnerSessionKey);

                const chatSession = new ChatPartner({
                    firstUserId: idChatId,
                    secondUserId: partnerIdChat,
                    whenTheyEnd: new Date() // Current time as end time
                });

                 chatSession.save();

            }




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

async function fetchActiveChatSessions() {
    const sessionKeys = await client.keys('chatSession:*');
    const sessions = {};

    for (const key of sessionKeys) {
        const partnerIdChat = await client.get(key);
        const userIdChat = key.split(':')[1]; // Assuming the key format is "chatSession:idChat.idchat"
        sessions[userIdChat] = partnerIdChat;
    }
    console.log(sessions);
    return sessions;
}


const PORT = process.env.PORT || 3002;
server.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});
