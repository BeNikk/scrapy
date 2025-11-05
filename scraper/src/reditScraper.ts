import { chromium } from "playwright";

interface SubRedittResult {
  id: string;
  title: string;
  url: string;
  subReditt: string;
  upvotes: number;
}
export async function scrapSubRedit(subRedit: string): Promise<any> {
  try {
    const browser = await chromium.launch({ headless: false, slowMo: 100 });  // Or 'firefox' or 'webkit'.
    const page = await browser.newPage();
    await page.goto('https://nikkhil.tech');
    // other actions...
    await browser.close();
  } catch (error) {
    console.log("error occured in scraping", error);
  }
}
scrapSubRedit("abc");
