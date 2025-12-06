## ADDED Requirements
### Requirement: Smart Money Analysis
The system SHALL provide Smart Money indicators to analyze institutional movements.

#### Scenario: Calculate SMI (Smart Money Index)
- **WHEN** calculating indicators for a stock
- **THEN** calculate SMI based on CLV (Close Location Value) and Volume
- **AND** generate a signal if SMI increases

#### Scenario: Calculate SVI (Smart Volume Index)
- **WHEN** calculating indicators
- **THEN** calculate SVI based on Volume/Range ratio
- **AND** generate a signal if SVI > 1.5 * MA20(SVI)

#### Scenario: Calculate NVI (Negative Volume Index)
- **WHEN** calculating indicators
- **THEN** calculate NVI which only changes when volume decreases
- **AND** generate a signal if NVI > MA20(NVI)

#### Scenario: VSA Signal (Volume Spread Analysis)
- **WHEN** volume is high (> 1.5 * AvgVol) AND price bar is a hammer (close near high)
- **THEN** identify as "Stopping Volume" (Signal 1)
- **WHEN** price hits limit up (> 9.5%)
- **THEN** identify as "Limit Up" (Signal 2)

#### Scenario: Smart Score
- **WHEN** all indicators are calculated
- **THEN** sum up the signals (SMI, SVI, NVI, VWAP, VSA) to get a Smart Score (0-5)

#### Scenario: Scanning
- **WHEN** user selects "Smart Money Scan"
- **THEN** filter stocks with Smart Score >= 3 OR VSA Signal > 0
- **AND** display results in the specified 4-line format
