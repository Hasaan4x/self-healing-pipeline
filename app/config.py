import os
from dotenv import load_dotenv
load_dotenv()

DB_URL = os.getenv("DB_URL", "postgresql+psycopg2://hasaan:dev@localhost:5432/healing")

# hourly data → give some slack
SLA_MINUTES = int(os.getenv("SLA_MINUTES", "120"))
NULL_THRESH = float(os.getenv("NULL_THRESH", "0.05"))
DUP_THRESH  = float(os.getenv("DUP_THRESH", "0.0"))

CITIES = [
    {"name": "London",     "lat": 51.5072, "lon": -0.1276},
    {"name": "Manchester", "lat": 53.4808, "lon": -2.2426},
    {"name": "Glasgow",    "lat": 55.8642, "lon": -4.2518},
]
