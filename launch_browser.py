from playwright.sync_api import sync_playwright
import time

def run():
    with sync_playwright() as p:
        print("Launching browser...")
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        print("Navigating to Google...")
        page.goto("https://www.google.com")
        print("Page title:", page.title())
        # Keep it open for a few seconds so the user can see it
        time.sleep(5)
        browser.close()
        print("Browser closed.")

if __name__ == "__main__":
    run()
