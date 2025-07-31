from src import (
    client_associate_id_update,
    data_importer,
    db_exporter,
    folder_db_exporter,
    folder_details_extraction,
    wc_fact_table_insertion
)
from src.utils.logger_config import AppLogger
from src.refresh_materialized_view import materialized_view_refresh

logger = AppLogger().get_logger()

def main():
    logger.info("Pipeline execution started")

    try:
        logger.info("Step 1: Importing sheet data from drive")
        df = data_importer.get_sheet_data_from_drive()
        logger.debug(f"Data imported: {df.shape[0]} rows")

        logger.info("Step 2: Extracting folder details")
        file_ = folder_details_extraction.details_extractions()
        logger.debug(f"Extracted folder file: {file_}")

        logger.info("Step 3: Uploading data to the database")
        db_exporter.uploader(df)
        logger.info("Data uploaded successfully")

        logger.info("Step 4: Uploading folder data to DB")
        folder_db_exporter.upload(file_)
        logger.info("Folder data uploaded successfully")

        logger.info("Step 5: Updating client-associate ID mapping")
        client_associate_id_update.update_client_associate_data()
        logger.info("Client-associate mapping updated")

        logger.info("Step 6: Updating fact table")
        wc_fact_table_insertion.update_fact_table()
        logger.info("fact table update complete")

        logger.info("Step 7: Refreshing Matrialized View")
        materialized_view_refresh()
        logger.info("Refreshed Matrialized View")


    except Exception as e:
        logger.exception("Pipeline execution failed due to an unexpected error")

    else:
        logger.info("Pipeline execution completed successfully")

if __name__ == '__main__':
    main()
