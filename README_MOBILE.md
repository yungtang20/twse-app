# Mobile (Termux) Setup Guide

## 1. Install Termux (Android)
Download Termux from F-Droid (Google Play version is outdated).

## 2. Install Dependencies in Termux
Run the following commands:

```bash
pkg update && pkg upgrade
pkg install python chromium git
# Optional: pkg install build-essential binutils (if compiling needed)
```

## 3. Transfer Files
Copy the following files to your phone (e.g., via USB or cloud):
- `backfill_tdcc_selenium.py`
- `stock_list.csv`
- `processed_stocks.txt`
- `requirements.txt`
- `taiwan_stock.db` (Optional: You can start with a new empty DB and merge later)

## 4. Install Python Libraries
In the folder where you copied the files:

```bash
pip install -r requirements.txt
```

## 5. Run the Scripts

## 5. Run the Script
To repair the missing price data (fill the gaps):
```bash
python repair_missing_data.py
```

That's it! Once finished, your database will be 100% complete.

## Notes
- **Database**: If you don't copy `taiwan_stock.db`, the script will create a new one. You can copy this new DB back to your PC later and merge it.
- **Rate Limiting**: The script is already configured with delays to avoid IP bans.
- **Background**: Termux might stop if the screen is off. Acquire a wakelock or keep the screen on.
