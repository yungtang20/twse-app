# Plugin System Specification

## ADDED Requirements

### Requirement: Plugin Management
The system SHALL provide a plugin management system that allows users to create, edit, delete, and execute custom stock scanning plugins.

#### Scenario: Load plugins on startup
- **WHEN** the application starts
- **THEN** it SHALL load all built-in plugins from `default_plugins.json`
- **AND** load all user-created plugins from `user_plugins.json`

#### Scenario: Create new plugin
- **WHEN** user clicks the "Add Custom Plugin" button
- **THEN** a plugin editor screen SHALL be displayed
- **AND** user can input Python code following the `scan(data, params)` template

#### Scenario: Save user plugin
- **WHEN** user saves a plugin in the editor
- **THEN** the plugin SHALL be validated for syntax errors
- **AND** stored in `user_plugins.json`

### Requirement: Plugin Execution Sandbox
The system SHALL execute user plugins in a secure sandbox environment that prevents dangerous operations.

#### Scenario: Block dangerous imports
- **WHEN** a plugin attempts to use `import os`, `import sys`, `open()`, or `eval()`
- **THEN** the execution SHALL be blocked
- **AND** an error message SHALL be displayed

#### Scenario: Provide safe helper functions
- **WHEN** a plugin is executed
- **THEN** it SHALL have access to safe helper functions:
  - `safe_float(val)` - Safe numeric conversion
  - `filter_by_volume(data, min_vol)` - Volume filtering
  - `sort_results(data, key, desc)` - Result sorting

### Requirement: AI Plugin Generation
The system SHALL provide AI-assisted plugin generation using Google Gemini API.

#### Scenario: Generate plugin from natural language
- **WHEN** user inputs "Create a scan for RSI < 30"
- **THEN** the system SHALL call Gemini API with a specialized prompt
- **AND** display the generated Python code for user review

#### Scenario: Store Gemini API Key
- **WHEN** user enters their Gemini API Key in settings
- **THEN** it SHALL be stored in `gemini_config.json`
- **AND** used for all AI generation requests

### Requirement: K-Line Chart Display
The system SHALL display interactive candlestick charts with technical indicators.

#### Scenario: Display daily candlestick chart
- **WHEN** user views a stock's detail page
- **THEN** a candlestick chart SHALL be displayed with OHLC data
- **AND** daily/weekly/monthly period toggle buttons SHALL be available

#### Scenario: Toggle moving averages
- **WHEN** user toggles MA3/MA20/MA60/MA120/MA200 buttons
- **THEN** the corresponding moving average lines SHALL be shown or hidden on the chart

#### Scenario: Display volume subplot
- **WHEN** the chart is displayed
- **THEN** a volume bar chart SHALL be shown below the main chart
- **AND** bars SHALL be colored red (up) or green (down)

### Requirement: Custom Moving Average
The system SHALL allow users to define custom moving average periods.

#### Scenario: Add custom MA line
- **WHEN** user clicks the "+ Custom MA" button
- **THEN** a dialog SHALL appear for entering period (1-500)
- **AND** selecting color and line style
