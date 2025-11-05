import { chromium } from "playwright";

export let lastSeen = new Set<string>();

export interface GitHubRepo {
  id: string;
  name: string;
  owner: string;
  description: string;
  language: string;
  stars: number;
  forks: number;
  starsToday: number;
  url: string;
  scrapedAt: string;
}

export async function fetchTrendingRepos(): Promise<GitHubRepo[]> {
  const browser = await chromium.launch({ headless: false });
  const page = await browser.newPage();

  
  try {
    await page.goto('https://github.com/trending', { waitUntil: 'networkidle' });
    
    const repos = await page.$$eval('article.Box-row', (articles) => {
      return articles.map((article) => {
        const titleElement = article.querySelector('h2 a');
        const fullName = titleElement?.getAttribute('href')?.replace('/', '') || '';
        const [owner, name] = fullName.split('/');
        
        const description = article.querySelector('p')?.textContent?.trim() || '';
        const language = article.querySelector('[itemprop="programmingLanguage"]')?.textContent?.trim() || '';
        
        const statsElements = article.querySelectorAll('a.Link--muted');
        let stars = 0;
        let forks = 0;
        
        statsElements.forEach(el => {
          const text = el.textContent?.trim() || '';
          if (el.getAttribute('href')?.includes('/stargazers')) {
            stars = parseInt(text.replace(',', '')) || 0;
          }
          if (el.getAttribute('href')?.includes('/forks')) {
            forks = parseInt(text.replace(',', '')) || 0;
          }
        });
        
        const starsToday = parseInt(
          article.querySelector('span.d-inline-block.float-sm-right')?.textContent?.match(/(\d+)/)?.[1] || '0'
        );
        
        return {
          id: fullName,
          name: name || '',
          owner: owner || '',
          description,
          language,
          stars,
          forks,
          starsToday,
          url: `https://github.com${titleElement?.getAttribute('href')}`,
          scrapedAt: new Date().toISOString()
        };
      });
    });
    console.log("repos",repos);
    console.log(`Scraped ${repos.length} trending repos`);
    return repos.filter(repo => repo.id);
    
  } finally {
    await browser.close();
  }
}