"""Run the Adzuna extraction once (lands raw JSON to bronze).

Usage (repo root, venv active):
    python run_adzuna.py
Requires ADZUNA_APP_ID and ADZUNA_APP_KEY in your .env.
"""

import logging

from dotenv import load_dotenv

load_dotenv()  # put ADZUNA_* and AWS creds from .env into the environment

from pulse.extractors.adzuna import AdzunaExtractor  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s | %(message)s")

if __name__ == "__main__":
    path = AdzunaExtractor().run()
    print(f"Bronze written: {path}")
