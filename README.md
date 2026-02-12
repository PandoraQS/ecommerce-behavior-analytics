# E-commerce Behavioral Analytics Platform

An end-to-end data engineering project designed to ingest, process, and visualize user behavioral patterns. This project simulates a Solutions Engineering workflow: transforming raw integration data into actionable behavioral insights.

## Overview

The platform identifies behavioral patterns, such as high-frequency impulsive actions, by processing simulated e-commerce clickstream data through a robust ETL pipeline.

## Tech Stack

- Language: Python 3.x
- Data Processing: Pandas
- Storage: SQL (SQLite)
- Visualization: Streamlit and Plotly
- Data Generation: Faker

## Architecture

1. Data Ingestion: A generator creates raw JSON logs containing user actions, timestamps, and categories. It includes intentional data quality issues, such as missing values, to test pipeline robustness.
2. Data Validation Layer: Implemented Pydantic models to enforce type-safety and business logic (e.g., non-negative transaction amounts) before processing. This ensures only high-quality data reaches the transformation stage.
3. ETL Pipeline:
    - Extracts raw JSON data.
    - Handles data validation and cleaning of ISO8601 timestamps.
    - Performs Behavioral Feature Engineering: identifies high-frequency actions defined as events occurring less than 2 seconds apart.
4. Storage: Loads the refined dataset into a relational SQL database.
5. Analytics Dashboard: A real-time web interface to monitor user risk and activity distribution.

## Key Feature: Risk Detection

The system specifically flags impulsive behavior patterns. In a professional context, such as gambling harm prevention or fraud detection, this logic serves as a foundation for identifying users showing a potential loss of control or automated bot activity.

## Getting Started

1. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

2. Generate raw data:

   ```bash
   pip install -r requirements.txt
   ```

3. Run the ETL pipeline:

    ```bash
    python src/processor.py
    ```

4. Launch the dashboard:

    ```bash
    streamlit run app.py
    ```

## Project Structure

- data/: Directory for raw logs and the SQLite database.
- src/data_generator.py: Script for generating synthetic behavioral data.
- src/processor.py: The ETL pipeline logic with schema validation.
- app.py: The Streamlit web application.
