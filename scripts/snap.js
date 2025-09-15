// scripts/snap.js
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright');

(async () => {
  const infile = process.argv[2] || 'layer1/targets.txt';
  const outdir = process.argv[3] || 'layer1/snaps';
  fs.mkdirSync(outdir, { recursive: true });

  // read lines, ignore empty & pure comment lines
  const urls = fs
    .readFileSync(infile, 'utf8')
    .split(/\r?\n/)
    .map(s => s.trim())
    .filter(s => s && !s.startsWith('#'));

  const browser = await chromium.launch();
  const context = await browser.newContext({
    viewport: { width: 1366, height: 900 },
    userAgent:
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36',
    extraHTTPHeaders: {
      'Accept': 'text/html,application/json,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      'Accept-Language': 'en-US,en;q=0.9',
      'Upgrade-Insecure-Requests': '1',
    },
  });

  for (const url of urls) {
    const slug = url.replace(/[^a-z0-9]+/gi, '_').slice(0, 120);
    const base = path.join(outdir, slug);

    const page = await context.newPage();

    const nets = [];
    let firstJsonBody = null;

    page.on('response', async (resp) => {
      try {
        const rurl = resp.url();
        const headers = resp.headers();
        const ct = (headers['content-type'] || '').toLowerCase();

        // Prefer decoded text; Playwright handles br/gzip
        let bodyText = null;
        try { bodyText = await resp.text(); } catch { bodyText = null; }
        if (!bodyText) {
          const buf = await resp.body();
          bodyText = Buffer.from(buf).toString('utf-8');
        }

        nets.push({
          url: rurl,
          status: resp.status(),
          headers,
          body: bodyText,
        });

        // Keep first JSON-looking body for quick ingest fast-path
        const looksJson =
          ct.includes('json') ||
          /\b_json\b/i.test(rurl) ||
          /_format=json/i.test(rurl) ||
          /\.json(\?|$)/i.test(rurl);

        if (!firstJsonBody && looksJson && bodyText && bodyText.trim().length) {
          firstJsonBody = bodyText;
        }
      } catch {
        /* ignore individual response failures */
      }
    });

    const meta = { url, started_at: new Date().toISOString() };
    try {
      const resp = await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 });
      meta.final_url = page.url();
      meta.status = resp ? resp.status() : null;

      // If main navigation itself is JSON, persist that body too.
      try {
        const mainCT = (resp && (resp.headers()['content-type'] || '').toLowerCase()) || '';
        if (mainCT.includes('json')) {
          // main response body as text
          let mainText = null;
          try { mainText = await resp.text(); } catch { mainText = null; }
          if (!mainText) {
            const buf = await resp.body();
            mainText = Buffer.from(buf).toString('utf-8');
          }
          if (mainText && mainText.trim().length) {
            firstJsonBody = firstJsonBody || mainText;
          }
        }
      } catch { /* ignore */ }

      // Gentle scroll (helps JS sites render numbers further down)
      for (let i = 0; i < 6; i++) {
        await page.mouse.wheel(0, 1200);
        await page.waitForTimeout(800);
      }

      // Artifacts
      await page.screenshot({ path: base + '.viewport.png', fullPage: false });
      await page.screenshot({ path: base + '.full.png', fullPage: true });

      // Only write HTML if this is actually HTML
      try {
        const ctMain = (resp && (resp.headers()['content-type'] || '').toLowerCase()) || '';
        if (ctMain.includes('html')) {
          fs.writeFileSync(base + '.html', await page.content(), 'utf8');
        } else {
          // Fallback: still write DOM content for debugging
          fs.writeFileSync(base + '.html', await page.content(), 'utf8');
        }
      } catch {
        // best-effort
        fs.writeFileSync(base + '.html', await page.content(), 'utf8');
      }

      fs.writeFileSync(base + '.network.json', JSON.stringify(nets, null, 2));
      if (firstJsonBody) {
        fs.writeFileSync(base + '.body.json', firstJsonBody);
      }
      fs.writeFileSync(base + '.meta.json', JSON.stringify(meta, null, 2));

      console.log('SNAP OK', url);
    } catch (e) {
      meta.error = String(e && e.message ? e.message : e);
      fs.writeFileSync(base + '.meta.json', JSON.stringify(meta, null, 2));
      console.log('SNAP ERR', url, e.message || e);
    } finally {
      await page.close();
      // polite pacing between targets
      await new Promise(r => setTimeout(r, 1000));
    }
  }

  await context.close();
  await browser.close();
})();
