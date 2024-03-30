const express = require('express');
const http = require('http');
const socketIo = require('socket.io');

// Create a new express application
const app = express();

// Create an HTTP server and wrap the Express app
const server = http.createServer(app);

// Attach socket.io to the server
const io = socketIo(server);

// Serve static files from the public directory (optional)
app.use(express.static('public'));

// Handle WebSocket connections
io.on('connection', (socket) => {
    console.log('A user connected');

    // Listen for a chat request and handle it
    socket.on('requestChat', async () => {
        console.log('User requested a chat');
        // Implement your logic for finding a chat partner here
        // Placeholder response
        const partnerId = 'partner123'; // Assume you find a partner
        socket.emit('chatPartnerFound', { partnerId });
    });
});

// Set the port and start the server
const PORT = process.env.PORT || 3002;
server.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});
