# Kivy App Feature Migration Specification

## ADDED Requirements

### Requirement: Stock Scanning Module
The Kivy App SHALL implement all 7 scanning functions from `最終修正.py`.

#### Scenario: VP Scan
- **WHEN** user selects VP Scan
- **THEN** the app SHALL query stocks near support/resistance levels
- **AND** display results with Volume Profile indicators

#### Scenario: MFI Scan
- **WHEN** user selects MFI Scan
- **THEN** the app SHALL filter stocks by Money Flow Index
- **AND** categorize as overbought (>80), oversold (<20), or neutral

#### Scenario: MA Alignment Scan
- **WHEN** user selects MA Alignment Scan
- **THEN** the app SHALL find stocks where all 5 MAs (3/20/60/120/200) are aligned bullishly

#### Scenario: Smart Money Scan
- **WHEN** user selects Smart Money Scan
- **THEN** the app SHALL filter stocks with Smart Score >= 3
- **AND** display SMI/SVI/NVI/VSA signals

### Requirement: Unified Scan Result Display
The Kivy App SHALL display scan results in a consistent 4-line format.

#### Scenario: Display scan result entry
- **WHEN** a scan completes successfully
- **THEN** each result SHALL be displayed as:
  - Line 1: Date, Name(Code), Volume, MFI
  - Line 2: Close price, POC, 14-day change, Smart Score
  - Line 3: Take profit, VWAP, Stop loss, Signals
  - Line 4: MA3, MA20, MA60, MA120, MA200

#### Scenario: Navigate to stock detail
- **WHEN** user taps a scan result
- **THEN** the app SHALL navigate to the stock's detail page with K-line chart

### Requirement: Stock Query Screen
The Kivy App SHALL provide a stock query feature on the home screen.

#### Scenario: Search stock by code
- **WHEN** user enters a stock code (e.g., "2330")
- **THEN** the app SHALL fetch and display the stock's latest data
- **AND** show a K-line chart with technical indicators

#### Scenario: Add to watchlist
- **WHEN** user taps the "Add to Watchlist" button
- **THEN** the stock SHALL be saved to the user's watchlist
- **AND** persist across app restarts

### Requirement: Cloud Data Source
The Kivy App SHALL support reading data from Supabase cloud.

#### Scenario: Toggle cloud/local mode
- **WHEN** user toggles the cloud switch in settings
- **THEN** the app SHALL switch between Supabase and local SQLite data sources

#### Scenario: Download cloud data to local
- **WHEN** user taps "Download to Local" button
- **THEN** the app SHALL sync cloud data to local SQLite database
