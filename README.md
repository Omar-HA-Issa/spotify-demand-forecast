# Spotify Demand Forecasting Pipeline

**Status:** 🚧 In Development

End-to-end ML pipeline for predicting daily streaming demand for music tracks.

## Overview

This project builds a production-grade demand forecasting system using AWS infrastructure, dbt transformations, and PyTorch GRU models to predict daily streaming demand for music tracks.

### Key Features
- Synthetic time-series data generation from Spotify metadata
- Cloud-native data warehouse (AWS Redshift Serverless)
- SQL-based feature engineering with dbt
- PyTorch GRU neural network for time-series forecasting
- Automated data quality validation
- Interactive dashboard for predictions

## Tech Stack

**Cloud Infrastructure:**
- AWS S3 (data lake)
- AWS Redshift Serverless (data warehouse)
- AWS SageMaker (ML training)
- AWS IAM (access management)

**Data Engineering:**
- dbt (data transformations)
- SQL (feature engineering)

**Machine Learning:**
- Python 3.10+
- PyTorch (GRU model)
- pandas, numpy (data processing)

**Visualization:**
- QuickSight / Tableau

## Project Structure
```
spotify-forecast/
├── data/                           # Raw data (not tracked)
│   └── spotify_tracks.csv
├── spotify_demand_forecast/        # dbt project
│   ├── models/
│   │   ├── staging/               # Cleaned raw data
│   │   ├── intermediate/          # Feature engineering
│   │   └── mart/                  # Final ML-ready tables
│   ├── tests/                     # Data quality tests
│   └── dbt_project.yml
├── scripts/                       # Python utilities
│   ├── generate_synthetic_data.py # Create time-series data
│   ├── train_gru.py              # Model training
│   └── validate_data.py          # Quality checks
├── notebooks/                     # Exploratory analysis
│   └── eda.ipynb
├── .gitignore
└── README.md
```

## Setup Instructions

### Prerequisites
- AWS account with Redshift Serverless access
- Python 3.10+
- Git

### Installation

**1. Clone the repository**
```bash
git clone https://github.com/YOUR_USERNAME/spotify-demand-forecast.git
cd spotify-demand-forecast
```

**2. Set up Python environment**
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

**3. Configure dbt**
```bash
# Edit ~/.dbt/profiles.yml with your Redshift credentials
dbt debug  # Test connection
```

**4. Download Spotify dataset**
- Download from [Kaggle](https://www.kaggle.com/datasets/maharshipandya/-spotify-tracks-dataset)
- Place in `data/spotify_tracks.csv`

## Current Progress

- [x] AWS infrastructure setup (S3, Redshift, IAM)
- [x] GitHub repository initialized
- [ ] dbt project configuration
- [ ] Synthetic data generation
- [ ] Feature engineering models
- [ ] GRU model training
- [ ] Model evaluation
- [ ] Dashboard creation

## Timeline

**Week 1-2: Core Pipeline**
- Data generation and warehousing
- dbt transformations
- GRU model training
- Basic predictions

**Week 3: Extensions (Optional)**
- Real-time streaming (Kinesis)
- Automated retraining (Step Functions)
- Production API (Lambda)

## Results (Coming Soon)

TBD: Model performance metrics, dashboard screenshots, key findings

---

**Author:** Omar  
**Purpose:** Data Science / ML Engineering internship portfolio project  
**Timeline:** December 2024 - January 2025