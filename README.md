# 📊 Waste Cost Dashboard
A Streamlit dashboard to analyse **waste weight**, **waste cost (€)** and **DBU product share** over time.

The original internal version of this project connects to **Snowflake** (DBU weights) and **PostgreSQL** (inrooissy weights) and runs on real operational data.  
This public repository uses **anonymised CSV samples** instead, so it is safe to share and easy to run locally.

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

## 🔐 About Real Databases vs Demo Data

In the original internal project:
- Snowflake is used to query DBU weights
(table similar to DBU with columns like BOX_ID, PRODUIT, BOX_WEIGHT, etc.)

- PostgreSQL is used to query inrooissy warehouse weights
(tables like whs_box_operation and sale_order_box)

The two sources are merged, aggregated at a monthly level, and combined with monthly waste cost from Excel.

For this public GitHub version:
All live database connections are disabled by default.
The app loads pre-extracted and anonymised CSV files from the data/ folder:
df DBU.csv (DBU weights)
df roissy.csv (inrooissy weights)

The transformation and visualisation code is the same as in the internal project, but no real database credentials or sensitive business data are exposed.

The global flag in waste_cost_dashboard.py controls the behaviour:
USE_DEMO_DATA = True  # default for GitHub / public

True → use anonymised CSV data from data/ (recommended for sharing)
False → use real Snowflake & PostgreSQL connections defined in .streamlit/secrets.toml

## ▶️ How to Run (Demo Mode – no databases required)
Clone the repository:
git clone https://github.com/<your-username>/<your-repo>.git
cd <your-repo>

Create and activate a virtual environment (optional but recommended):
python -m venv .venv
source .venv/bin/activate    # on Windows: .venv\Scripts\activate

Install dependencies:
pip install -r requirements.txt

Ensure the following files exist:
data/df DBU.csv
data/df roissy.csv

Run the Streamlit app:
streamlit run waste_cost_dashboard.py

You do not need any database credentials for demo mode.

## ▶️ How to Run (Live Mode – with Snowflake & PostgreSQL)
If you want to connect the app to our own Snowflake / PostgreSQL instances (if you are my collegues):
Set the flag in waste_cost_dashboard.py:
USE_DEMO_DATA = False

Create a .streamlit/secrets.toml file based on the template
Make sure .streamlit/secrets.toml is ignored by git (see .gitignore) and never commit real credentials to GitHub.

Run the app as usual:
streamlit run waste_cost_dashboard.py

## 📸 Screenshot
![Dashboard Overview](docs/dashboard_overview.png)

## 🔧 Tech Stack
- Backend & Data
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

👩‍💻 Author
📘 MSc in Data Analytics and AI
📘 MSc in Statistics
🔍 Driven to transform raw data into strategic insights that lead to real impact.
📧 [xuefei.wang.fr@gmail.com]