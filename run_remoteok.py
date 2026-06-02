import logging
from pulse.extractors.remoteok import RemoteOKExtractor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s | %(message)s",
)

if __name__ == "__main__":
    path = RemoteOKExtractor().run()
    print(f"Done. Raw data landed at: {path}")
