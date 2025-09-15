const fs = require('fs');
const { chromium } = require('playwright');
(async () => {
  const url = process.argv[2];
  const out = process.argv[3];
  const browser = await chromium.launch();
  const context = await browser.newContext({
    userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36',
    extraHTTPHeaders: {
      'Accept': 'application/json, text/plain, */*',
      'Referer': 'https://www.powerball.com/',
      'Origin': 'https://www.powerball.com',
      'Accept-Language': 'en-US,en;q=0.9',
    },
  });
  const req = await context.request.get(url);
  const text = await req.text();
  fs.writeFileSync(out, text);
  await context.close();
  await browser.close();
  console.log('WROTE', out, 'bytes=', text.length);
})();
