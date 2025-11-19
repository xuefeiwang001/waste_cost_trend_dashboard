# 📊 Waste Cost Trend Dashboard
A real-world data project analyzing how warehouse inbound weight influences waste-processing costs.  

The dashboard combines operational data from **Snowflake** (DBU weights) and **PostgreSQL** (inroissy weights) to reveal:  
📦 How much inbound weight the warehouse processes each month  
🗑️ How waste-processing costs evolve over time  
📈 The monthly share of DBU-related weight and how it contributes to total cost  
🔍 Year-over-year trends in both weight and cost  
🎯 Key cost drivers and seasonal patterns  

For public release, the dashboard relies on **cleaned demo datasets** that replicate the real production structure while ensuring full data privacy.  

This project demonstrates skills in:  
- Data extraction & cleaning (SQL + Python)
- Multi-source data modelling & integration
- Interactive dashboard design
- Analytical storytelling driven by real operational data

---

## 🚀 Features
- Monthly aggregation of:
  - Total weight (DBU + other transporters)
  - DBU weight and DBU share (%)
  - Total waste cost (€)
- Combined visual:
  - **Stacked bars** for weight (DBU vs Other)
  - **Red dashed line** for cost
  - **Text labels** on bars for DBU ratio
- Per-month small multiples:
  - For each calendar month, a small chart comparing different years
- Caching for faster repeated runs

---

## 🧱 Project Structure
```text
.
├── waste_cost_dashboard.py        # Main Streamlit app
├── requirements.txt               # Python dependencies
├── data/
│   ├── df DBU.csv                 # Demo DBU data (extracted & anonymised)
│   └── df roissy.csv              # Demo inrooissy data (extracted & anonymised)
└── .streamlit/
    └── secrets_template.toml      # Example secrets file (no real credentials)

```

## 🔐 About Real Databases vs Demo Data
In the original internal project:  
- Snowflake is used to query DBU weights
(table similar to DBU with columns like BOX_ID, PRODUIT, BOX_WEIGHT, etc.)

- PostgreSQL is used to query inrooissy warehouse weights  
(tables like whs_box_operation and sale_order_box)

The two sources are merged, aggregated at a monthly level, and combined with monthly waste cost from Excel.

For this public GitHub version:  
- All live database connections are disabled by default.  
- The app loads pre-extracted and anonymised CSV files from the data/ folder:  
    - df DBU.csv (DBU weights)
    - df roissy.csv (inrooissy weights)

The transformation and visualisation code is the same as in the internal project, but no real database credentials or sensitive business data are exposed.  

The global flag in waste_cost_dashboard.py controls the behaviour:  
```python
USE_DEMO_DATA = True  # default for GitHub / public
```
- True → use anonymised CSV data from data/ (recommended for sharing)
- False → use real Snowflake & PostgreSQL connections defined in .streamlit/secrets.toml

## ▶️ How to Run (Demo Mode – no databases required)
Clone the repository:
```git
git clone https://github.com/<your-username>/<your-repo>.git
cd <your-repo>
```

Create and activate a virtual environment (optional but recommended):
```git
python -m venv .venv
source .venv/bin/activate    # on Windows: .venv\Scripts\activate
```
Install dependencies:
```python
pip install -r requirements.txt
```

Ensure the following files exist:
data/df DBU.csv
data/df roissy.csv

```bash
Run the Streamlit app:
streamlit run waste_cost_dashboard.py
```
Upload the required Excel file in the app UI
The dashboard requires a price Excel file named:  
```kotlin
2024-2025 data.xlsx
```
You will upload this file directly inside the Streamlit UI when prompted.    

You do not need any database credentials for demo mode.

## ▶️ How to Run (Live Mode – with Snowflake & PostgreSQL)
If you want to connect the app to our own Snowflake / PostgreSQL instances (if you are my collegues):  
Set the flag in waste_cost_dashboard.py:
```python
USE_DEMO_DATA = False
```

Create a .streamlit/secrets.toml file based on the template.   
Make sure .streamlit/secrets.toml is ignored by git (see .gitignore) and never commit real credentials to GitHub.  
Run the app as usual:
```bash
streamlit run waste_cost_dashboard.py
```

Upload the required Excel file in the app UI  
The dashboard requires a price Excel file named:    
```kotlin
2024-2025 data.xlsx
```
You will upload this file directly inside the Streamlit UI when prompted.   

## 📸 Screenshot
<img width="1916" height="941" alt="dashboard overview" src="https://github.com/user-attachments/assets/be488efc-fe08-47e8-95ca-d6d279af54aa" />

## 🔧 Tech Stack
Backend & Data
- Python 3.x
- Pandas
- NumPy
- Snowflake Connector for Python
- Psycopg2 (PostgreSQL)
- Regex / I/O utilities

Visualization
- Streamlit
- Altair

Dev Tools
- Git + GitHub
- Pre-commit (black, ruff, isort)
- Virtual environment (venv or conda)

## 📄 License
This project is shared under the MIT License.

## 💡 Notes
- All CSV data in this repository has been sampled and anonymised and does not contain confidential business information.
- The focus of this project is to demonstrate:
    - Data extraction and integration from multiple sources
    - Aggregation logic (monthly weight, DBU share, cost)
    - Interactive visualisation and dashboard design with Streamlit + Altair

## 👩‍💻 Author  
🎓 Dual Master's Degrees in Data Science  
  - MSc in Data Analytics & Artificial Intelligence  
  - MSc in Statistics  

📌 Profile Summary  
A data-driven professional with strong analytical foundations and hands-on experience in end-to-end data projects — from ETL & dashboarding to statistical modelling and AI-powered insights.   
Passionate about transforming raw operational data into actionable business strategies.

📧 Contact: xuefei.wang.fr@gmail.com




