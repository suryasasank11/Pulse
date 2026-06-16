"""Run the USAJobs extraction once (lands raw JSON to bronze).

Usage (repo root, venv active):
    python run_usajobs.py
Requires USAJOBS_API_KEY and USAJOBS_EMAIL in your .env.
"""
import logging

from dotenv import load_dotenv

load_dotenv()

from pulse.extractors.usajobs import USAJobsExtractor  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s | %(message)s")

if __name__ == "__main__":
    path = USAJobsExtractor().run()
    print(f"Bronze written: {path}")