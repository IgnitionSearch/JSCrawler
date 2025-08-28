const express = require('express');
const mysql = require('mysql2/promise');
const cors = require('cors');
const { exec } = require('child_process');

const app = express();
app.use(cors());
app.use(express.json());

const dbConfig = {
  host: '159.89.234.226',
  user: 'root',
  password: '123',
  database: 'seo_crawls',
  port: 32002, 
};

const pool = mysql.createPool(dbConfig);

// Start crawl endpoint
app.get('/start-crawl', (req, res) => {
  exec('node crawler.js', (err, stdout) => {
    if (err) return res.status(500).send('Error: ' + err.message);
    res.send('Crawl started: ' + stdout);
  });
});

// Get pages
app.get('/pages', async (req, res) => {
  const [rows] = await pool.query('SELECT * FROM pages');
  res.json(rows);
});

// Get links
app.get('/links', async (req, res) => {
  const [rows] = await pool.query('SELECT * FROM links');
  res.json(rows);
});

// Get resources
app.get('/resources', async (req, res) => {
  const [rows] = await pool.query('SELECT * FROM resources');
  res.json(rows);
});

// New root route
app.get('/', (req, res) => {
  res.send('Server is running');
});

const PORT = 3000;
app.listen(PORT, () => console.log('Server running on http://localhost:' + PORT));