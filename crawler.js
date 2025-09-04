// crawler.js
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
    'INSERT INTO crawls (start_time, start_url) VALUES (NOW(), ?)',
    [crawl_url]
  );
  const crawlId = ins.insertId;

  const crawler = new PlaywrightCrawler({
    launchContext: { launchOptions: { headless: true } },
    maxRequestsPerCrawl: 10,
    maxRequestRetries: 3,

    async requestHandler({ page, request, enqueueLinks, log }) {
      log.info(`[${crawlId}] Processing ${request.url}...`);

      //HEAD first to avoid loading heavy pages unnecessarily
      const headResp = await page.request.fetch(request.url, { method: 'HEAD' }).catch(() => null);
      const statusCode = headResp ? headResp.status() : null;
      const contentType = headResp ? (headResp.headers()['content-type'] || null) : null;
      const contentLength = headResp ? parseInt(headResp.headers()['content-length'] || '0', 10) : 0;

      let title = null, metaDescription = null, canonical = null, metaRobots = null, hreflang = null;

      //DOM only parsse for html tables
      if (contentType && contentType.includes('text/html')) {
        await page.goto(request.url, { waitUntil: 'domcontentloaded' }).catch(() => {});

        try { title = await page.title(); } catch {}

        try {
          metaDescription = await page.$eval('meta[name="description"]', el => el.content);
        } catch {}

        try {
          canonical = await page.$eval('link[rel="canonical"]', el => el.href);
        } catch {}

        try {
          metaRobots = await page.$eval('meta[name="robots"]', el => el.content);
        } catch {}

        try {
          hreflang = await page.$$eval('link[rel="alternate"]',
            els => els.map(el => el.getAttribute('hreflang')).filter(Boolean).join(', ')
          );
        } catch {}
      }

      const pageExtra = { };

    
      const pageInsertSql = `
        INSERT INTO url
          (crawl_id, url, status_code, content_type, content_length, title, meta_description, canonical, meta_robots, hreflang, extra_data, size)
        VALUES
          (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE
          status_code     = VALUES(status_code),
          content_type    = VALUES(content_type),
          content_length  = VALUES(content_length),
          title           = VALUES(title),
          meta_description= VALUES(meta_description),
          canonical       = VALUES(canonical),
          meta_robots     = VALUES(meta_robots),
          hreflang        = VALUES(hreflang),
          extra_data      = VALUES(extra_data),
          size            = VALUES(size),
          id              = LAST_INSERT_ID(id)`;

        const [pageRes] = await pool.execute(pageInsertSql, [
        crawlId, request.url, statusCode, contentType, contentLength,
        title, metaDescription, canonical, metaRobots, hreflang,
        JSON.stringify(pageExtra), contentLength
      ]);

      const pageId = pageRes.insertId;

      
      
        //anchor links
        const anchorLinks = await page.$$eval('a', as =>
          as.map(a => a.href).filter(Boolean)
        ).catch(() => []);

        for (const href of anchorLinks) {
          await pool.execute(
            `INSERT INTO links
              (crawl_id, from_page_id, to_url, alt_text, lazy_load, url_type)
            VALUES (?, ?, ?, NULL, NULL, 'anchor')
            ON DUPLICATE KEY UPDATE
              to_url = VALUES(to_url),
              url_type = VALUES(url_type)`,
            [crawlId, pageId, href]
          ).catch(err => log.error(`Link save error ${href}: ${err.message}`));
        }

        //alt and lazy loading for images
        const imgNodes = await page.$$eval('img', imgs =>
          imgs.map(img => ({
            src: img.currentSrc || img.src || img.getAttribute('src') || '',
            alt: (img.getAttribute('alt') || '').trim(),
            loading: (img.getAttribute('loading') || '').toLowerCase() || null,
          })).filter(i => i.src)
        ).catch(() => []);

        const imgSummaries = [];

        for (const im of imgNodes) {
        await pool.execute(
          `INSERT INTO links
            (crawl_id, from_page_id, to_url, alt_text, lazy_load, url_type)
          VALUES (?, ?, ?, ?, ?, 'image')
          ON DUPLICATE KEY UPDATE
            to_url    = VALUES(to_url),
            alt_text  = VALUES(alt_text),
            lazy_load = VALUES(lazy_load),
            url_type  = VALUES(url_type)`,
          [crawlId, pageId, im.src, im.alt || null, im.loading || 'eager']).catch(err => log.error(`Image save error ${im.src}: ${err.message}`));

          // Probe headers (best-effort)
          let imgStatus = null, imgType = null, imgLen = 0;
          try {
            let r = await fetch(im.src, { method: 'HEAD' });
            if (!r.ok || !r.headers.get('content-length')) {
              r = await fetch(im.src, { method: 'GET', headers: { Range: 'bytes=0-0' } });
            }
            imgStatus = r.status;
            imgType   = r.headers.get('content-type');
            imgLen    = parseInt(r.headers.get('content-length') || '0', 10);
          } catch {}

          imgSummaries.push({
            url: im.src,
            status: imgStatus,
            type: imgType,
            size: imgLen,
            alt: im.alt,
            loading: im.loading
          });
        }
        //keep crawling in the same origin
        await enqueueLinks({ globs: [`${new URL(request.url).origin}/**`] });
      
    },

    async failedRequestHandler({ request, log }) {
      log.error(`Failed ${request.url} after retries: ${request.errorMessages}`);
    },
  });

  try {
    await crawler.run([crawl_url]);
  } finally {
    await pool.execute('UPDATE crawls SET end_time = NOW() WHERE id = ?', [crawlId]);
    await pool.end();
  }
}

module.exports = { runCrawler };
