from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
import traceback

try:
    print("Initializing options...")
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    
    print("Installing driver...")
    driver_path = ChromeDriverManager().install()
    print(f"Driver path: {driver_path}")
    
    print("Starting service...")
    service = Service(driver_path)
    
    print("Creating driver...")
    driver = webdriver.Chrome(service=service, options=options)
    
    print("Driver created successfully!")
    driver.quit()
except Exception as e:
    print("Error:")
    print(e)
    traceback.print_exc()
