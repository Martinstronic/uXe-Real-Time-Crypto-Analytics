## Author 

**Patrick Azevedo** Automation & Data Engineer LinkedIn: https://www.linkedin.com/in/patrick-azevedo/ 

Crypto market intelligence platform built with Node.js, HTML, JavaScript, Binance APIs, real-time indicators, and Telegram alert automation.

## Live Demo
https://uxe-crypto-analytics.onrender.com

> Note: the instance may take a few seconds to wake up due to free hosting limitations.

# uXe Crypto Analytics

Real-time crypto market analytics platform designed to monitor market structure, detect high-impact events, and support data-driven decision making through automated alerts, multi-timeframe indicators, and live dashboards.

## Overview

uXe Crypto Analytics is a web-based market intelligence platform built to collect, process, and visualize cryptocurrency market data in real time using Binance APIs.

The platform was created to help users monitor the behavior of major trading pairs through a single operational dashboard, combining price action, volume, funding rate, open interest, long/short sentiment, RSI, custom indicators, and alerting logic.

It was designed with a strong focus on:

- real-time monitoring
- operational efficiency
- multi-timeframe analysis
- event detection
- automated alerting
- scalable analytics workflows

## Key Features

- Real-time BTC monitoring cards
- Multi-asset dashboard with ranking table
- Binance API integration (multiple endpoints)
- Multi-timeframe analysis for price, RSI, candles, and volume
- Open Interest variation tracking across multiple intervals
- Long/Short Ratio (LSR), LSAR, and LSPR monitoring
- Funding Rate tracking
- Custom event detection engine
- Telegram alert system with 4 alert categories
- Internal alert panel inside the dashboard
- User filter presets and profile saving
- Performance and health monitoring panel
- Modular backend logic for future AI/ML expansion

## Main Indicators Tracked

The platform consolidates and displays multiple market metrics, including:

- Price
- Price variation across multiple timeframes
- Volume across multiple timeframes
- Open Interest (OI)
- OI percentage variation in different intervals
- Funding Rate
- Long/Short Ratio (LSR)
- LSAR / LSPR market sentiment indicators
- RSI across multiple timeframes
- Custom RSI Delta indicator
- Multi-timeframe candle direction
- Event detection (capitulation, buying/selling pressure, etc.)

## Custom Logic

### RSI Delta
A custom indicator created to compare RSI behavior and volume strength against BTC, aiming to identify relative asset strength and momentum divergence.

> This logic is currently under refinement and may evolve into a more advanced momentum scoring model.

### Event Engine
The platform includes a rule-based event system that highlights relevant market conditions such as:

- capitulation
- buying pressure
- selling pressure
- abnormal OI/volume behavior

This module is also planned for future improvements.

## Architecture

### Frontend
- HTML
- JavaScript
- Interactive dashboard layout
- Live data updates via WebSocket
- User-side filtering and profile selection

### Backend
- Node.js
- Express
- Binance API integrations
- Data aggregation and normalization
- Real-time distribution layer
- Telegram integration for alerting
- Internal logic for indicator calculation and event generation

### Deployment
- Local execution supported
- Cloud deployment via Render
- Git-connected deployment workflow

## Tech Stack

- Node.js
- Express
- JavaScript
- HTML/CSS
- WebSocket
- Binance Public APIs
- Telegram Bot API
- Render

## Data Sources

Current primary data source:

- Binance public APIs

Planned future integrations:

- Coinglass
- Additional premium market intelligence APIs
- Expanded liquidation and derivatives data providers

## Alerting System

The platform sends automated Telegram alerts based on market conditions, including OI and volume behavior.

Current alert logic includes:

- multiple severity levels
- OI and volume spikes
- threshold-based market monitoring
- internal visual alert panel

## Use Cases

This platform is intended for:

- crypto market monitoring
- discretionary trading support
- signal validation
- alert-based opportunity detection
- market behavior exploration
- analytics experimentation

## Future Roadmap

Planned improvements include:

- richer liquidation data integrations
- improved event classification
- upgraded RSI Delta model
- machine learning experiments in Python
- anomaly detection
- better performance panel placement and UX
- more advanced alert configuration
- expanded profile and watchlist management

## Security Notes

Sensitive credentials and authentication details are not exposed in the repository.  
Secrets should be stored in environment variables and excluded from version control.
