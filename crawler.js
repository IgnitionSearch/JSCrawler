const { PlaywrightCrawler } = require('crawlee');
const mysql = require('mysql2/promise');
require('dotenv').config();

const dbConfig = {
  host: process.env.host,
  user: process.env.user,
  password: process.env.password,
  database: process.env.database,
  port: process.env.port, 
};

const pool = mysql.createPool(dbConfig);

async function runCrawler(crawl_url) {
  const crawler = new PlaywrightCrawler({
    launchContext: { launchOptions: { headless: true } },
    maxRequestsPerCrawl: 20,
    maxRequestRetries: 3,

    async requestHandler({ page, request, enqueueLinks, log }) {
      log.info(`Processing ${request.url}...`);
      const crawlId = 4;

      // Get basic headers via Playwright (works even for images)
      const headResp = await page.request.fetch(request.url, { method: 'HEAD' });
      const statusCode = headResp.status();
      const contentType = headResp.headers()['content-type'] || null;
      const contentLength = parseInt(headResp.headers()['content-length'] || '0', 10);

      // Navigate only if it's likely HTML; otherwise title/meta will fail
      let title = null, metaDescription = null, canonical = null, metaRobots = null, hreflang = null;
      if (contentType && contentType.includes('text/html')) {
        await page.goto(request.url, { waitUntil: 'domcontentloaded' });
        title = await page.title();
        try {
          metaDescription = await page.$eval('meta[name="description"]', el => el.content).catch(() => null);
          canonical = await page.$eval('link[rel="canonical"]', el => el.href).catch(() => null);
          metaRobots = await page.$eval('meta[name="robots"]', el => el.content).catch(() => null);
          hreflang = await page.$$eval('link[rel="alternate"]', els =>
            els.map(el => el.getAttribute('hreflang')).filter(Boolean).join(', ')
          );
        } catch (e) {
          log.warning(`Meta extract issue on ${request.url}: ${e.message}`);
        }
      }

      const extraData = JSON.stringify({}); // put any extra per-page info here

      // 1) Insert/Upsert the page row ONCE and get pageId
      // Use LAST_INSERT_ID trick so we always get the id on duplicate
      const insertSql = `
        INSERT INTO url
          (crawl_id, url, status_code, content_type, content_length, title, meta_description, canonical, meta_robots, hreflang, extra_data, size)
        VALUES
          (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE
          status_code = VALUES(status_code),
          content_type = VALUES(content_type),
          content_length = VALUES(content_length),
          title = VALUES(title),
          meta_description = VALUES(meta_description),
          canonical = VALUES(canonical),
          meta_robots = VALUES(meta_robots),
          hreflang = VALUES(hreflang),
          extra_data = VALUES(extra_data),
          size = VALUES(size),
          id = LAST_INSERT_ID(id)`;
      // use contentLength as "size" for the page itself
      await pool.execute(insertSql, [
        crawlId, request.url, statusCode, contentType, contentLength,
        title, metaDescription, canonical, metaRobots, hreflang, extraData, contentLength
      ]);
      const [[{ id: pageId }]] = await pool.query('SELECT LAST_INSERT_ID() AS id');

      // 2) Collect images (only makes sense for HTML pages)
      let imageUrls = [];
      if (contentType && contentType.includes('text/html')) {
        imageUrls = await page.$$eval('img', imgs => imgs.map(img => img.src).filter(Boolean));
      }
      log.info(`Found ${imageUrls.length} images on ${request.url}`);

      // Optionally: store image sizes in extra_data or another table.
      // Here we append a small summary into extra_data.
      const sizedImages = [];
      for (const imgUrl of imageUrls) {
        let imgSize = 0;
        try {
          // HEAD may fail; fall back to GET range
          let r = await fetch(imgUrl, { method: 'HEAD' });
          if (!r.ok || !r.headers.get('content-length')) {
            r = await fetch(imgUrl, { method: 'GET', headers: { Range: 'bytes=0-0' } });
          }
          imgSize = parseInt(r.headers.get('content-length') || '0', 10);
        } catch { /* ignore */ }
        sizedImages.push({ url: imgUrl, size: imgSize });
      }
      if (sizedImages.length) {
        const newExtra = JSON.stringify({ images: sizedImages.slice(0, 50) }); // avoid huge rows
        await pool.execute(
          'UPDATE url SET extra_data = ? WHERE id = ?',
          [newExtra, pageId]
        );
      }

      // 3) Links (only for HTML)
      if (contentType && contentType.includes('text/html')) {
        const linkUrls = await page.$$eval('a', as => as.map(a => a.href).filter(Boolean));
        for (const linkUrl of linkUrls) {
          try {
            await pool.execute(
              'INSERT INTO links (crawl_id, from_page_id, to_url) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE to_url = VALUES(to_url)',
              [crawlId, pageId, linkUrl]
            );
          } catch (error) {
            log.error(`Error saving link ${linkUrl}: ${error.message}`);
          }
        }
      }

      await enqueueLinks({ globs: [`${new URL(request.url).origin}/**`] });
    },

    async failedRequestHandler({ request, log }) {
      log.error(`Failed ${request.url} after retries: ${request.errorMessages}`);
    },
  });

  try {
    await crawler.run([crawl_url]);
  } finally {
    await pool.end();
  }
}

// Example usage:
// runCrawler('https://www.hypnotherapysheffield.net/');

module.exports = { runCrawler };
