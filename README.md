# SF Housing Intelligence

**Should I rent this place?**

A cloud-hosted web app that ingests real San Francisco building inspection data, computes risk signals via KMeans clustering, and serves searchable renter insights through a serverless AWS backend.

**Live:** [https://main.d2niwtrbg801n0.amplifyapp.com](https://main.d2niwtrbg801n0.amplifyapp.com)

---

## What It Does

SF renters have no easy way to assess a building's history before signing a lease. This app surfaces public violation data from the Department of Building Inspection and answers one question: *should I rent this place?*

- **50,000 SF buildings** scored by violation history, recency, and severity
- **40 neighborhoods** ranked and clustered by risk level
- **Real-time search** by address or neighborhood
- **KMeans clustering** (k=3) classifies buildings as Stable, Transitional, or High-Risk
- Risk scores computed from open violation density, recency weighting, severity, and building age

---

## Stack

| Layer | Technology |
|---|---|
| Frontend | React + Vite, deployed on AWS Amplify |
| API | AWS API Gateway + Lambda (Node.js) |
| Database | PostgreSQL on AWS RDS (PostGIS enabled) |
| Data pipeline | Python ETL — SF Open Data → RDS |
| ML | KMeans clustering (scikit-learn) |
| Infrastructure | AWS CDK (TypeScript) |

---

## Architecture

```
SF Open Data API
      │
      ▼
  ETL (Python)          ← pulls violations + assessments, computes risk scores
      │
      ▼
RDS PostgreSQL           ← buildings, violations, neighborhood_stats tables
      │
      ▼
Lambda Functions         ← neighborhood, building, search endpoints
      │
      ▼
API Gateway              ← https://0t5tmbp5ub.execute-api.us-west-2.amazonaws.com/prod
      │
      ▼
React Frontend           ← https://main.d2niwtrbg801n0.amplifyapp.com
```

---

## API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| GET | `/neighborhoods` | All 42 neighborhoods sorted by risk score |
| GET | `/neighborhoods/:name` | Single neighborhood with full violation breakdown |
| GET | `/building?blklot=` | Building profile, violations, similar buildings |
| GET | `/search?q=&type=address\|neighborhood` | Autocomplete search |

---

## Data Sources

- **Violations:** [SF DBI Complaints](https://data.sfgov.org/resource/gm2e-bten.json)
- **Assessments:** [SF Property Assessment Roll](https://data.sfgov.org/resource/wv5m-vpq2.json)

---

## Local Development

```bash
# Frontend
cd frontend
npm install
echo "VITE_API_BASE_URL=https://0t5tmbp5ub.execute-api.us-west-2.amazonaws.com/prod" > .env
npm run dev

# ETL (requires Python + DB credentials in data/.env)
cd data
python etl.py
```

---

## Project Structure

```
sf-housing-analytics/
├── frontend/              # Vite + React app
│   └── src/App.jsx
├── backend/
│   └── lambdas/
│       ├── building/      # Building profile endpoint
│       ├── neighborhood/  # Neighborhood data endpoint
│       └── search/        # Search autocomplete endpoint
├── data/
│   └── etl.py             # ETL pipeline
└── infrastructure/
    └── cdk/               # AWS CDK stack (TypeScript)
```

---

## Resume Bullet

> Built a cloud-hosted San Francisco housing intelligence app that ingests public violation data, computes building risk signals via KMeans clustering, and serves searchable renter insights through a serverless AWS backend (API Gateway, Lambda, RDS PostgreSQL) and React frontend on Amplify.