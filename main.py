"""
Entry point — run the EL pipeline end to end.
Usage:
    python main.py
"""
import logging
from dotenv import load_dotenv

load_dotenv()

from pipeline.config import settings
from pipeline.extractor import APIExtractor
from pipeline.loader import DuckDBLoader

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def run():
    extractor = APIExtractor(
        base_url=settings.api_base_url,
        api_key=settings.api_key,
    )

    with DuckDBLoader(db_path=settings.db_path) as loader:
        all_records = []

        for page in extractor.extract("/endpoint"):   # swap endpoint once spec is known
            all_records.extend(page)

        count = loader.load(all_records, table_name="raw_records")
        logger.info("Pipeline complete — %d total rows loaded", count)


if __name__ == "__main__":
    run()
