from src import client_associate_id_update, data_importer, db_exporter, folder_db_exporter, folder_details_extraction, wc_fact_table_insertion

def main():
    df = data_importer.get_sheet_data_from_drive()           
    file_ = folder_details_extraction.details_extractions()  
    db_exporter.uploader(df)                                 
    folder_db_exporter.upload(file_)                        
    
    client_associate_id_update.update_client_associate_data() 
    wc_fact_table_insertion.update_fact_table()                       


if __name__=='__main__':
    main()