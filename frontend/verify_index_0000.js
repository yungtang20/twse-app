import { chromium } from 'playwright';

(async () => {
    try {
        console.log('Launching browser...');
        const browser = await chromium.launch();
        const page = await browser.newPage();

        page.on('console', msg => console.log('PAGE LOG:', msg.text()));

        console.log('Navigating to stock 0000 page...');
        await page.goto('http://localhost:5173/stock/0000');

        // Wait for price or name to appear
        console.log('Waiting for content...');
        try {
            await page.waitForSelector('h1', { timeout: 10000 });
            const title = await page.innerText('h1');
            console.log('Page Title (H1):', title);

            // Check for price
            const price = await page.innerText('.text-4xl'); // Assuming price has this class
            console.log('Price:', price);

            // Check for chart canvas
            const canvas = await page.$('canvas');
            console.log('Chart canvas found:', !!canvas);

        } catch (e) {
            console.log('Timeout waiting for content.');
            const body = await page.innerText('body');
            console.log('BODY SNAPSHOT:', body.substring(0, 500));
        }

        await browser.close();
        console.log('Verification complete.');

    } catch (error) {
        console.error('Error:', error);
        process.exit(1);
    }
})();
