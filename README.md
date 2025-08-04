# 🏀 Bounce Take-Home Assignment

This repository contains the solution to a data modeling and analytics challenge using **dbt**, **Python**, and **Looker Studio**. The project is structured to cover data transformation, exchange rate normalization, marketing attribution modeling, and business insight visualization.

---

## 🚀 Project Overview

**Challenge Summary**  
The assignment focuses on creating a modern data pipeline that supports marketing attribution and revenue analysis. It includes:

1. Data modeling using **dbt** (staging and marts layers)
2. Currency conversion using a **Python script** and exchange rate API
3. Attribution modeling (first-touch and last-touch)
4. A **Looker Studio dashboard** to visualize insights

---

## 🧱 Part 1: Transformation and Modeling

Using the provided spreadsheet, the following **dbt models** were created:

### 🔹 Staging Layer (`stg_` models)
- Cleaned and standardized:
  - `events`
  - `users`
  - `sessions`

### 🔹 Marts Layer (`mart_` models)
- Built fact and dimension models
- The `sessions` mart includes:
  - `session_start`
  - `session_end`
- Used tests for data quality

📁 See the `models/` folder for SQL logic and lineage.

---

## 💱 Part 2: Exchange Rate Normalization

A Python script fetches historical exchange rates from an **open-source API**, converting all revenue to **USD**.

### 🔧 Script Details
- Language: Python 3.x
- API: [Open Exchange Rates](https://openexchangerates.org)
- Output: CSV with original and converted revenue

The converted revenue is integrated into the `events` mart via an incremental `dbt` model.

📄 See [`fetch_exchange_rates.py`](bounce/fetch_exchange_rates.py)

---

## 🎯 Part 3: Attribution Modeling

Two marketing attribution models were built into the `marts` layer:

### 📌 First-Touch Attribution (30-day window)
- Assigns revenue to the **first UTM source** within 30 days prior to the order.

### 📌 Last-Touch Attribution
- Assigns revenue to the **most recent UTM source** before the order.

Each session is attributed to a `utm_source`, then orders are joined to determine attribution.

These metrics appear in the `fct_attribution_event_level` model.

---

## 📊 Part 4: Dashboard & Insights

### 📈 [👉 Live Dashboard (Looker Studio)](https://lookerstudio.google.com/embed/reporting/ca07df30-bb26-4c62-aa57-0cff6cd03351/page/p_rsary77zud)

> (If the embed doesn't load in your browser, [click here to open it in a new tab](https://lookerstudio.google.com/reporting/ca07df30-bb26-4c62-aa57-0cff6cd03351/page/p_rsary77zud))

### Key Metrics Displayed:
- Revenue by **First-Touch** vs **Last-Touch** UTM source
- **Total Revenue by Country**
- Additional charts showing session-level behavior and source breakdowns

### 📌 Why These Metrics Matter:
- Understand **top-performing acquisition channels**
- Compare **initial vs converting sources** to optimize marketing mix
- Identify **high-value countries** for geographic targeting

---

## 🛠️ Tech Stack

| Tool       | Purpose                         |
|------------|----------------------------------|
| dbt        | Data modeling & transformation   |
| Python     | Exchange rate extraction         |
| Looker Studio | Data visualization             |
| Git        | Version control                  |

---

## 📝 TODOs & Improvements

- [ ] Add automated testing for attribution logic
- [ ] Deploy dbt models with a scheduler (e.g., Airflow or dbt Cloud)
- [ ] Cache exchange rate API responses
- [ ] Add more granular UTM tracking dimensions

---

## 📂 Repo Structure

```bash
.
├── models/
│   ├── staging/
│   ├── marts/
├── seeds/
│   └── raw_events.csv
│   └── exchange_rates.csv
├── fetch_exchange_rates.py
├── README.md
```