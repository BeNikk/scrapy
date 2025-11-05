import playwright from 'playwright';

(async () => {
  for (const browserType of ['chromium']) {
    const browser = await playwright["chromium"].launch();
    const context = await browser.newContext();
    const page = await context.newPage();
    await page.goto('https://amazon.com');
    await page.screenshot({ path: `nodejs_${browserType}.png`, fullPage: true });
    await page.waitForTimeout(1000);
    await browser.close();
  }
})();
