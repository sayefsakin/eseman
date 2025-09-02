const http = require('http');
const fs = require('fs');
const path = require('path');

const PORT = 3000;
const filePath = path.join(__dirname, './vega.html'); // Adjust path if needed

const server = http.createServer((req, res) => {
  if (req.url === '/' || req.url === '/vega.html') {
    fs.readFile(filePath, (err, data) => {
      if (err) {
        res.writeHead(404, {'Content-Type': 'text/plain'});
        res.end('404 Not Found');
      } else {
        res.writeHead(200, {'Content-Type': 'text/html'});
        res.end(data);
      }
    });
  } else {
    res.writeHead(404, {'Content-Type': 'text/plain'});
    res.end('404 Not Found');
  }
});
// server.use(cors({origin: true, credentials: true}));

server.listen(PORT, '127.0.0.1', () => {
  console.log(`Server running at http://127.0.0.1:${PORT}/`);
});