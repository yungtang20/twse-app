import { chromium } from 'playwright';

(async () => {
    try {
        console.log('Launching browser...');
        const browser = await chromium.launch();
        console.log('Browser launched successfully!');
        const page = await browser.newPage();
        await page.goto('http://localhost:5173');
        console.log('Page title:', await page.title());
        await browser.close();
        console.log('Browser closed.');
    } catch (error) {
        console.error('Error launching browser:', error);
    }
})();
