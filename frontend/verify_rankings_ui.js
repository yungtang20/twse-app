import { chromium } from 'playwright';

(async () => {
    try {
        console.log('Launching browser...');
        const browser = await chromium.launch();
        const page = await browser.newPage();

        // Capture console logs
        page.on('console', msg => console.log('PAGE LOG:', msg.text()));
        page.on('pageerror', exception => console.log('PAGE ERROR:', exception));

        console.log('Navigating to rankings page...');
        await page.goto('http://localhost:5173/rankings');

        // Wait a bit for initial render
        await page.waitForTimeout(2000);

        console.log('Checking page state...');
        try {
            await page.waitForSelector('tbody tr', { timeout: 10000 });

            // Extract data
            const rows = await page.$$eval('tbody tr', (trs) => {
                return trs.slice(0, 5).map(tr => {
                    const tds = Array.from(tr.querySelectorAll('td'));
                    return tds.map(td => td.innerText.trim());
                });
            });

            console.log('Found ' + rows.length + ' rows. First 5 rows:');
            rows.forEach((row, index) => {
                console.log(`Row ${index + 1}: ${JSON.stringify(row)}`);
            });

        } catch (e) {
            console.log('Timeout waiting for table rows.');
            const bodyText = await page.innerText('body');
            console.log('BODY TEXT SNAPSHOT:\n', bodyText);
        }

        await browser.close();
        console.log('Verification complete.');

    } catch (error) {
        console.error('Error:', error);
        process.exit(1);
    }
})();
