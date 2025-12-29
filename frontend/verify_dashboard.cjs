const { chromium } = require('playwright');

(async () => {
    console.log('Starting Dashboard Verification...');
    let browser;
    try {
        browser = await chromium.launch({ headless: true });
        const page = await browser.newPage();

        page.on('console', msg => console.log('PAGE LOG:', msg.text()));
        page.on('pageerror', err => console.log('PAGE ERROR:', err.message));

        console.log('Navigating to http://localhost:5173...');
        await page.goto('http://localhost:5173', { timeout: 30000 });

        console.log('Checking for Dashboard content...');

        // Check for any h3
        try {
            await page.waitForSelector('h3', { timeout: 10000 });
            const h3Text = await page.locator('h3').first().textContent();
            console.log(`Found H3 Title: "${h3Text}"`);

            if (h3Text.includes('加權指數')) {
                console.log('✅ Title contains "加權指數"');
            } else {
                console.error('❌ Title does NOT contain "加權指數"');
            }
        } catch (e) {
            console.error('❌ No H3 element found.');
            const content = await page.content();
            console.log('Page content snippet:', content.substring(0, 500));
        }

        // Check for chart container
        const chartContainer = page.locator('.rounded-xl');
        if (await chartContainer.isVisible()) {
            console.log('✅ Chart container is visible');
        } else {
            console.error('❌ Chart container NOT visible');
        }

        // Check MA5 button
        const ma5Btn = page.locator('button', { hasText: 'MA5' });
        if (await ma5Btn.isVisible()) {
            await ma5Btn.click();
            console.log('✅ Clicked MA5 Button');
            await page.waitForTimeout(1000);
        } else {
            console.error('❌ MA5 Button not found');
        }

        // Check Sub-chart
        const subChartDiv = page.locator('div', { hasText: '圖表區域 (開發中)' }).last();
        if (await subChartDiv.isVisible()) {
            const text = await subChartDiv.textContent();
            console.log(`Sub-chart text: "${text}"`);
            if (text.includes('日KD') || text.includes('MACD')) {
                console.log('✅ Sub-chart title correct');
            } else {
                console.warn('⚠️ Sub-chart title check failed');
            }
        } else {
            console.error('❌ Sub-chart area not found');
        }

        console.log('🎉 Verification Finished.');

    } catch (error) {
        console.error('❌ Script Error:', error.message);
    } finally {
        if (browser) await browser.close();
    }
})();
