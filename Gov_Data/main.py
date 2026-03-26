from src.extract.extract import extract_from_s3
from src.transform.transform import transform_chunk
from src.load.load import load_to_staging, load_fact_tables
from src.dimensions.dimensions import load_all_dimensions
from src.utils.logger import get_logger
from sql.schema import run_schema
from dotenv import load_dotenv
import os

logger = get_logger()



def run():
    bucket = os.getenv("bucket")
    key = os.getenv("key")

    logger.info(">>> PIPELINE STARTED")

    run_schema()

    try:
        for i, chunk in enumerate(extract_from_s3(bucket, key), start=1):

            logger.info(f"1. Processing chunk {i}")

            # ---------------------
            # TRANSFORM
            # ---------------------

            logger.info("2. Transforming data")
            try:
                clean_chunk = transform_chunk(chunk)
                logger.info(f"      -Transform successful (chunk {i})")
            except Exception as e:
                logger.error(f"      -Transform failed (chunk {i}): {e}")
                continue

            # ---------------------
            # LOAD STAGING
            # ---------------------
            logger.info("3. Loading Staging data")
            try:
                load_to_staging(clean_chunk)
                logger.info(f"      -Staging load successful (chunk {i})")
            except Exception as e:
                logger.error(f"      -Staging load failed (chunk {i}): {e}")
                continue

            # ---------------------
            # LOAD DIMENSIONS
            # ---------------------
            logger.info("4. Loading Dimensions data")
            try:
                load_all_dimensions(clean_chunk)
                logger.info(f"      -Dimensions loaded (chunk {i})")
            except Exception as e:
                logger.error(f"      -Dimension load failed (chunk {i}): {e}")
                continue

            # ---------------------
            # LOAD FACT TABLES
            # ---------------------
            logger.info("5. Loading Fact Tables data")
            try:
                load_fact_tables(clean_chunk)
                logger.info(f"      -Fact tables loaded (chunk {i})\n ------------------------------------")
            except Exception as e:
                logger.error(f"      -Fact load failed (chunk {i}): {e}")
                continue

        logger.info(">>> PIPELINE COMPLETED SUCCESSFULLY\n ------------------------------------")
        print("PIPELINE COMPLETED")

    except Exception as e:
        logger.critical(f"!!! PIPELINE FAILED: {e}!!!")


if __name__ == "__main__":
    run()