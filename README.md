# Reputation & Trust Analytics Demo

A lightweight, "Analytics Engineering" demo showing how to build a Trust Score system for SME businesses.

## Business Context
We help SMEs understand their online reputation not just by "star rating" but by a composite **Trust Score** that measures:
1.  **Quality**: Time-weighted average rating (recent reviews matter more).
2.  **Engagement**: Response rate to customer reviews.
3.  **Volume**: Statistical significance of the feedback.

## Architecture
-   **Data Storage**: DuckDB (Serverless generic SQL warehouse)
-   **API**: FastAPI (Python)
-   **Frontend**: Vanilla JS + Chart.js + Leaflet.js
-   **Data Gen**: Faker + Python

## Key Metrics (SQL Models)
-   **Trust Score**: `(WeightedRating * 10) + (ResponseRate * 30) + (Log(Volume) * 20)` (Roughly)
-   **Industry Benchmarks**: P50 and P90 percentiles per industry.
-   **Geo Sentiment**: Spatial analysis of sentiment vs volume.

## How to Run
1.  **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```
2.  **Generate Data**:
    ```bash
    python3 src/data_gen.py
    ```
3.  **Start API & Dashboard**:
    ```bash
    python3 src/main.py
    ```
4.  **View**: Open `http://localhost:8000` in your browser.

## API Endpoints
-   `/api/business/{id}/overview`: KPI Summary.
-   `/api/business/{id}/deltas`: Activity vs previous 30 days.
-   `/api/geo/overview`: Geographic sentiment data.
-   `/api/geo/insight`: Automated spatial insights.
