const express = require('express');
const mysql = require('mysql2/promise');
const cors = require('cors');
const { exec } = require('child_process');
const { runCrawler } = require('./crawler');
require('dotenv').config();

const app = express();
app.use(cors());
app.use(express.json());

const dbConfig = {
  host: process.env.host,
  user: process.env.user,
  password: process.env.password,
  database: process.env.database,
  port: process.env.port, 
};

const pool = mysql.createPool(dbConfig);

// Start crawl endpoint
app.get('/start-crawl', async (req, res) => {
  const { crawl_url: encodedUrl } = req.query;
  const crawl_url = encodedUrl ? decodeURIComponent(encodedUrl) : undefined;
  if (!crawl_url) {
    return res.status(400).send('Missing crawl_url parameter');
  }
  try {
    const result = await runCrawler(crawl_url);
    res.send('Crawl started: ' + JSON.stringify(result));
  } catch (err) {
    res.status(500).send('Error: ' + err.message);
  }
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