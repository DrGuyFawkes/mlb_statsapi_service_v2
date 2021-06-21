import logging
from dataclasses import asdict
import ast
from service import s3, redshift_create_table, redshift
import sys

LOGGER = logging.getLogger(__name__)

def main(event, environment):
    """
    Entrypoint to service.
    
    :param event: AWS Event Trigger
    :type event: Dict[str, Any]
    :param environment: Dictionary containing env variables.
    :type environment: Dict[str, Any]
    """
    
    LOGGER.info(event)
    execution_event = {
        'status': '',
        'description': '',
        'metadata': {}
    }
    try:
        for record in event['Records']:
            bucket_name = record['s3']['bucket']['name']
            object_key = record['s3']['object']['key'].replace('%3D', '=')

            if 'data/rds_ingestion_data/' in object_key:

                stage_name = object_key.split('/')[-1].split('.')[0]
                target_name = object_key.split('/')[2]

                ## read a csv from s3 and write a dataframe
                df_csv = s3.read_csv_from_s3(bucket_name, object_key)
          
                ## create stage table in redshift
                redshift_credentials = environment['secretsmanager']['REDSHIFT_CREDENTIALS']
                response = redshift_create_table.create_stage_table(redshift_credentials, target_name, stage_name)
                response = redshift.insert_csv_into_stage(redshift_credentials, stage_name, df_csv)
                
                ### there is an issue. Not enough time to fix it. So I sumbit a second option. Ideally, upsert is a better approach.
                ## get column name from stage tale
                # columns = redshift.get_column_name(redshift_credentials, stage_name)            
                ## upsert data from stage to target table
                # response = redshift.upsert_data(redshift_credentials, target_name, stage_name, columns)

                ## upsert data from stage to target table
                response = redshift.merge_operation_by_replacing_existing_rows(redshift_credentials, target_name, stage_name)

                ## drop stage table
                response = redshift.drop_table(redshift_credentials, stage_name) 
               
                LOGGER.info('complete import process:: ', object_key)
            else:
                LOGGER.info('nothing to process: not target folder')

    except KeyError as e:
        error = f"Missing required field {e}."
        LOGGER.error(error)
        execution_event.update({"status": "Error", "description": error, "metadata": event})

        # if error, drop stage table
        #response = redshift.drop_table(redshift_credentials, stage_name) 
        sys.exit(1)

    except Exception as e:
        LOGGER.error(e)
        execution_event.update({"status": "Error", "description": f"{str(e)}", "metadata": event})

        # if error, drop stage table
        #response = redshift.drop_table(redshift_credentials, stage_name)
        sys.exit(1)

    finally:
        LOGGER.info(execution_event)

    return execution_event

   