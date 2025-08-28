const { PlaywrightCrawler } = require('crawlee');
const mysql = require('mysql2/promise');
require('dotenv').config();


async function runCrawler(crawl_url) {
  
  const pool = mysql.createPool({
    host: process.env.host,
    user: process.env.user,
    password: process.env.password,
    database: process.env.database,
    port: process.env.port,
  });
  const [ins] = await pool.execute(
    'INSERT INTO crawls (start_time) VALUES (NOW())'
  );
  const crawlId = ins.insertId;

  const crawler = new PlaywrightCrawler({
    launchContext: { launchOptions: { headless: true } },
    maxRequestsPerCrawl: 20,
    maxRequestRetries: 3,

    async requestHandler({ page, request, enqueueLinks, log }) {
      log.info(`[${crawlId}] Processing ${request.url}...`);

      const headResp = await page.request.fetch(request.url, { method: 'HEAD' });
      const statusCode = headResp.status();
      const contentType = headResp.headers()['content-type'] || null;
      const contentLength = parseInt(headResp.headers()['content-length'] || '0', 10);

      let title = null, metaDescription = null, canonical = null, metaRobots = null, hreflang = null;
      if (contentType && contentType.includes('text/html')) {
        // page is usually already at request.url, but this is safe:
        await page.goto(request.url, { waitUntil: 'domcontentloaded' });
        title = await page.title();
        metaDescription = await page.$eval('meta[name="description"]', el => el.content).catch(() => null);
        canonical      = await page.$eval('link[rel="canonical"]', el => el.href).catch(() => null);
        metaRobots     = await page.$eval('meta[name="robots"]', el => el.content).catch(() => null);
        hreflang       = await page.$$eval('link[rel="alternate"]',
        els => els.map(el => el.getAttribute('hreflang')).filter(Boolean).join(', '));
      }

      const extraData = JSON.stringify({}); // room for per-page extras

      // Upsert page row once; capture pageId
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
      await pool.execute(insertSql, [
        crawlId, request.url, statusCode, contentType, contentLength,
        title, metaDescription, canonical, metaRobots, hreflang, extraData, contentLength
      ]);
      const [[{ id: pageId }]] = await pool.query('SELECT LAST_INSERT_ID() AS id');

      if (contentType && contentType.includes('text/html')) {
        // Images → stash basic info in extra_data (optional)
        const imageUrls = await page.$$eval('img', imgs => imgs.map(i => i.src).filter(Boolean));
        const sizedImages = [];
        for (const imgUrl of imageUrls) {
          let imgSize = 0;
          try {
            let r = await fetch(imgUrl, { method: 'HEAD' });
            if (!r.ok || !r.headers.get('content-length')) {
              r = await fetch(imgUrl, { method: 'GET', headers: { Range: 'bytes=0-0' } });
            }
            imgSize = parseInt(r.headers.get('content-length') || '0', 10);
          } catch {}
          sizedImages.push({ url: imgUrl, size: imgSize });
        }
        if (sizedImages.length) {
          await pool.execute('UPDATE url SET extra_data = ? WHERE id = ?',
            [JSON.stringify({ images: sizedImages.slice(0, 50) }), pageId]);
        }

        // Links
        const linkUrls = await page.$$eval('a', as => as.map(a => a.href).filter(Boolean));
        for (const linkUrl of linkUrls) {
          await pool.execute(
            'INSERT INTO links (crawl_id, from_page_id, to_url) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE to_url = VALUES(to_url)',
            [crawlId, pageId, linkUrl]
          ).catch(err => log.error(`Link save error ${linkUrl}: ${err.message}`));
        }

        await enqueueLinks({ globs: [`${new URL(request.url).origin}/**`] });
      }
    },

    async failedRequestHandler({ request, log }) {
      log.error(`Failed ${request.url} after retries: ${request.errorMessages}`);
    },
  });

  try {
    await crawler.run([crawl_url]);
  } finally {
    // 2) mark crawl end time
    await pool.execute('UPDATE crawls SET end_time = NOW() WHERE id = ?', [crawlId]);
    await pool.end();
  }
}

module.exports = { runCrawler };
