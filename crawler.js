// crawler.js
const { PlaywrightCrawler } = require('crawlee');
const mysql = require('mysql2/promise');
require('dotenv').config();

/** ---------- helpers ---------- **/
const absolutize = (u, base) => {
  try {
    const url = new URL(u, base);
    url.hash = ''; // strip fragment
    return url.toString();
  } catch {
    return null;
  }
};

const classifyScope = (targetUrl, pageUrl) => {
  try {
    const t = new URL(targetUrl);
    const p = new URL(pageUrl);
    return (t.protocol.startsWith('http') && t.origin === p.origin) ? 'internal' : 'external';
  } catch {
    return 'external';
  }
};

/** ---------- main ---------- **/
async function runCrawler(crawl_url) {
  const pool = mysql.createPool({
    host: process.env.host,
    user: process.env.user,
    password: process.env.password,
    database: process.env.database,
    port: process.env.port,
  });

  //start crawl
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
      log.info(`Crawl ID: ${crawlId} | Processing URL: ${request.url}`);

      
      const headResp = await page.request.fetch(request.url, { method: 'HEAD' }).catch(() => null);
      const statusCode = headResp ? headResp.status() : null;
      const contentType = headResp ? (headResp.headers()['content-type'] || null) : null;
      const contentLength = headResp ? parseInt(headResp.headers()['content-length'] || '0', 10) : 0;

      let title = null, metaDescription = null, canonical = null, metaRobots = null, hreflang = null;

      
      const isHtml = contentType && contentType.includes('text/html');
      if (isHtml) {
        await page.goto(request.url, { waitUntil: 'domcontentloaded' }).catch(() => {});

        try { title = await page.title(); } catch {}
        try { metaDescription = await page.$eval('meta[name="description"]', el => el.content); } catch {}
        try { canonical = await page.$eval('link[rel="canonical"]', el => el.href); } catch {}
        try { metaRobots = await page.$eval('meta[name="robots"]', el => el.content); } catch {}
        try {
          hreflang = await page.$$eval('link[rel="alternate"]',
            els => els.map(el => el.getAttribute('hreflang')).filter(Boolean).join(', ')
          );
        } catch {}
      }

      const pageExtra = {}; 

      
      const pageInsertSql = `
        INSERT INTO url
          (crawl_id, url, status_code, content_type, content_length, title, meta_description, canonical, meta_robots, hreflang, extra_data, size)
        VALUES
          (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE
          status_code      = VALUES(status_code),
          content_type     = VALUES(content_type),
          content_length   = VALUES(content_length),
          title            = VALUES(title),
          meta_description = VALUES(meta_description),
          canonical        = VALUES(canonical),
          meta_robots      = VALUES(meta_robots),
          hreflang         = VALUES(hreflang),
          extra_data       = VALUES(extra_data),
          size             = VALUES(size),
          id               = LAST_INSERT_ID(id)
      `;

      const [pageRes] = await pool.execute(pageInsertSql, [
        crawlId, request.url, statusCode, contentType, contentLength,
        title, metaDescription, canonical, metaRobots, hreflang,
        JSON.stringify(pageExtra), contentLength
      ]);

      const pageId = pageRes.insertId;

      if (isHtml) {
        //anchor links
        const rawHrefs = await page.$$eval('a', as =>
          as.map(a => a.getAttribute('href')).filter(Boolean)
        ).catch(() => []);

        for (const raw of rawHrefs) {
          const href = absolutize(raw, request.url);
          if (!href) continue;
          const link_scope = classifyScope(href, request.url);

          await pool.execute(
            `INSERT INTO links
              (crawl_id, from_page_id, to_url, alt_text, lazy_load, url_type, link_scope)
             VALUES (?, ?, ?, NULL, NULL, 'anchor', ?)
             ON DUPLICATE KEY UPDATE
               to_url     = VALUES(to_url),
               url_type   = VALUES(url_type),
               link_scope = VALUES(link_scope)`,
            [crawlId, pageId, href, link_scope]
          ).catch(err => log.error(`Link save error ${href}: ${err.message}`));
        }

        //image alt and lazy loading
        const imgNodes = await page.$$eval('img', imgs =>
          imgs.map(img => ({
            src: img.getAttribute('src') || img.currentSrc || img.src || '',
            alt: (img.getAttribute('alt') || '').trim(),
            loading: (img.getAttribute('loading') || 'eager').toLowerCase(),
          })).filter(i => i.src)
        ).catch(() => []);

        for (const im of imgNodes) {
          const src = absolutize(im.src, request.url);
          if (!src) continue;
          const link_scope = classifyScope(src, request.url);

          await pool.execute(
            `INSERT INTO links
              (crawl_id, from_page_id, to_url, alt_text, lazy_load, url_type, link_scope)
             VALUES (?, ?, ?, ?, ?, 'image', ?)
             ON DUPLICATE KEY UPDATE
               to_url     = VALUES(to_url),
               alt_text   = VALUES(alt_text),
               lazy_load  = VALUES(lazy_load),
               url_type   = VALUES(url_type),
               link_scope = VALUES(link_scope)`,
            [crawlId, pageId, src, im.alt || null, im.loading || 'eager', link_scope]
          ).catch(err => log.error(`Image save error ${src}: ${err.message}`));
        }

        //crawls same origin
        await enqueueLinks({ globs: [`${new URL(request.url).origin}/**`] }).catch(() => {});
      }
    },

    async failedRequestHandler({ request, log }) {
      log.error(`Failed ${request.url} after retries: ${request.errorMessages}`);
    },
  });

  try {
    await crawler.run([crawl_url]);
  } finally {
    await pool.execute('UPDATE crawls SET end_time = NOW() WHERE id = ?', [crawlId]).catch(() => {});
    await pool.end().catch(() => {});
  }
}

module.exports = { runCrawler };
