# Databricks notebook source
# DBTITLE 1,notebook variables
# Define database connection parameters

source_conn_info = {
    "dbname": "staging-new",
    "user": "REDACTED",
    "password": "REDACTED",
    "host": "campaign-app.postgres.database.azure.com",
    "port": "****"
}

source_schema = "public"

dest_conn_info = {
    "dbname": "heavy_processes",
    "user": "REDACTED",
    "password": "REDACTED",

    "host": "main-processing.postgres.database.azure.com",
    "port": "****"
}

dest_schema = "analytical_model"

update_log_table = "update_log"

# COMMAND ----------

# DBTITLE 1,important setup
import psycopg2
import datetime

# COMMAND ----------

# DBTITLE 1,campaign
# Define the table to copy data from and to
table_name = "campaign"
key = "id_campaign"

try:
    # Connect to destination database to get the latest ingest timestamp
    dest_conn = psycopg2.connect(**dest_conn_info)
    dest_cursor = dest_conn.cursor()

    # Fetch latest ingest timestamp for 'version'
    dest_cursor.execute(f'''
        SELECT MAX(ingest_start_ts) FROM analytical_model.{update_log_table}
        WHERE "table" = %s
    ''', (table_name,))
    latest_ingest_start_ts = dest_cursor.fetchone()[0]

    # If no previous ingestion, default to a very old date
    if latest_ingest_start_ts is None:
        latest_ingest_start_ts = datetime.datetime(2000, 1, 1)

    # Record the new start timestamp
    ingest_start_ts = datetime.datetime.utcnow().replace(tzinfo=None)

    # Connect to source database
    source_conn = psycopg2.connect(**source_conn_info)
    source_cursor = source_conn.cursor()

    # Fetch only modified records since last ingest
    source_query = f'''
            SELECT CAST(id as int8) AS id_campaign, 
                name AS campaign_name, 
                audience AS audience_desc, 
                '' AS primary_objective, 
                current_status as status, 
                planned_start_dt as planned_start_dte, 
                planned_end_dt as planned_end_dte, 
                actual_start_dt as actual_start_dte, 
                actual_end_dt as actual_end_dte, 
                CASE WHEN update_dt IS NULL THEN created_dt ELSE update_dt END as modified_ts
            FROM paign_default_campaign 
            WHERE COALESCE(update_dt, created_dt) > '{latest_ingest_start_ts}'
        '''
    
    source_cursor.execute(source_query, (latest_ingest_start_ts,))
    rows = source_cursor.fetchall()
    
    if not rows:
        print("No new or updated records found. No changes made.")
    else:
        # Extract column names
        columns = [desc[0] for desc in source_cursor.description]
        column_names = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(columns))
        
        # Construct update clause dynamically
        update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col != key])

        # Extracting IDs for logging
        record_ids = [str(row[0]) for row in rows]  # row[0] = id_treatment
        record_ids_str = ', '.join(record_ids)  # Convert list to comma-separated string

        # Upsert query with ON CONFLICT
        upsert_query = f'''
            INSERT INTO analytical_model.{table_name} ({column_names})
            VALUES ({placeholders})
            ON CONFLICT ({key}) DO UPDATE SET {update_clause};
        '''

        # Bulk execute upsert query
        dest_cursor.executemany(upsert_query, rows)
        dest_conn.commit()

        # Record end timestamp
        ingest_end_ts = datetime.datetime.utcnow().replace(tzinfo=None)

        # Log ingestion including IDs
        log_query = f'''
            INSERT INTO analytical_model.{update_log_table} ("table", ingest_start_ts, ingest_end_ts, "type", "count", ids)
            VALUES (%s, %s, %s, %s, %s, %s);
        '''
        dest_cursor.execute(log_query, (table_name, ingest_start_ts, ingest_end_ts, "upsert", len(rows), record_ids_str))
        dest_conn.commit()

        print(f"Upserted {len(rows)} records in {table_name}")
        print("Update log recorded.")

    # Close connections
    source_cursor.close()
    source_conn.close()
    dest_cursor.close()
    dest_conn.close()

except Exception as e:
    print("Error:", e)

# COMMAND ----------

# DBTITLE 1,tactic
# Define the table to copy data from and to
table_name = "tactic"
key = "id_tactic"

try:
    # Connect to destination database to get the latest ingest timestamp
    dest_conn = psycopg2.connect(**dest_conn_info)
    dest_cursor = dest_conn.cursor()

    # Fetch latest ingest timestamp for 'version'
    dest_cursor.execute(f'''
        SELECT MAX(ingest_start_ts) FROM analytical_model.{update_log_table}
        WHERE "table" = %s
    ''', (table_name,))
    latest_ingest_start_ts = dest_cursor.fetchone()[0]

    # If no previous ingestion, default to a very old date
    if latest_ingest_start_ts is None:
        latest_ingest_start_ts = datetime.datetime(2000, 1, 1)

    # Record the new start timestamp
    ingest_start_ts = datetime.datetime.utcnow().replace(tzinfo=None)

    # Connect to source database
    source_conn = psycopg2.connect(**source_conn_info)
    source_cursor = source_conn.cursor()

    # Fetch only modified records since last ingest
    source_query = f'''
        SELECT 
        t.id AS id_tactic,
        t.campaign_id AS id_campaign,
        case when audience_criteria is null then 'Members with upcoming arrivals at the ' || brand_name else audience_criteria end as audience_desc, --was for lpa, cleanup
        name AS tactic_name,
        '' AS tactic_objective,
        cast(actual_start_dt as date) AS tactic_start_dte,
        cast(actual_end_dt as date) AS tactic_end_dte, 
        'In-Market' as tactic_status, -- double check this with Kyle, WF status needs to be reflected in CP
        tactic_type AS tactic_channel,
        'Batch' AS tactic_setup_type, --verify that this metadata is being collected
        'Marketing' AS tactic_type, --verify that this metadata is being collected
        'PCM' AS tactic_publisher, --verify that this metadata is being collected
        cast(planned_start_dt as date) AS planned_start_dte,
        cast(planned_end_dt as date) AS planned_end_dte,
        update_dt as modified_ts
        FROM paign_default_tactic t  
        WHERE COALESCE(update_dt, created_dt) > '{latest_ingest_start_ts}'
    '''
    source_cursor.execute(source_query, (latest_ingest_start_ts,))
    rows = source_cursor.fetchall()
    
    if not rows:
        print("No new or updated records found. No changes made.")
    else:
        # Extract column names
        columns = [desc[0] for desc in source_cursor.description]
        column_names = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(columns))
        
        # Construct update clause dynamically
        update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col != key])

        # Extracting IDs for logging
        record_ids = [str(row[0]) for row in rows]  
        record_ids_str = ', '.join(record_ids)  # Convert list to comma-separated string

        # Upsert query with ON CONFLICT
        upsert_query = f'''
            INSERT INTO analytical_model.{table_name} ({column_names})
            VALUES ({placeholders})
            ON CONFLICT ({key}) DO UPDATE SET {update_clause};
        '''

        # Bulk execute upsert query
        dest_cursor.executemany(upsert_query, rows)
        dest_conn.commit()

        # Record end timestamp
        ingest_end_ts = datetime.datetime.utcnow().replace(tzinfo=None)

        # Log ingestion including IDs
        log_query = f'''
            INSERT INTO analytical_model.{update_log_table} ("table", ingest_start_ts, ingest_end_ts, "type", "count", ids)
            VALUES (%s, %s, %s, %s, %s, %s);
        '''
        dest_cursor.execute(log_query, (table_name, ingest_start_ts, ingest_end_ts, "upsert", len(rows), record_ids_str))
        dest_conn.commit()

        print(f"Upserted {len(rows)} records in {table_name}")
        print("Update log recorded.")

    # Close connections
    source_cursor.close()
    source_conn.close()
    dest_cursor.close()
    dest_conn.close()

except Exception as e:
    print("Error:", e)

# COMMAND ----------

# DBTITLE 1,version
# Define table details
table_name = "version"
key = "id_version"
update_log_table = "update_log"

try:
    # Connect to destination database to get the latest ingest timestamp
    dest_conn = psycopg2.connect(**dest_conn_info)
    dest_cursor = dest_conn.cursor()

    # Fetch latest ingest timestamp for 'version'
    dest_cursor.execute(f'''
        SELECT MAX(ingest_start_ts) FROM analytical_model.{update_log_table}
        WHERE "table" = %s
    ''', (table_name,))
    latest_ingest_start_ts = dest_cursor.fetchone()[0]

    # If no previous ingestion, default to a very old date
    if latest_ingest_start_ts is None:
        latest_ingest_start_ts = datetime.datetime(2000, 1, 1)

    # Record the new start timestamp
    ingest_start_ts = datetime.datetime.utcnow().replace(tzinfo=None)

    # Connect to source database
    source_conn = psycopg2.connect(**source_conn_info)
    source_cursor = source_conn.cursor()

    # Fetch only modified records since last ingest
    source_query = f'''
        WITH ppv_data AS (
            SELECT 
                ppv.id AS id_version,
                ppv.tactic_id AS id_tactic,
                ppv.module_id,
                ppv.vehicle_placement_position_id,
                COALESCE(ppv.language, 'English Global Default') AS language_desc,
                ppv.audience_segment AS audience_segment_desc,
                ppv.name AS version_name,
                ppv.start_date AS planned_start_dte,
                ppv.end_date AS planned_end_dte,
                ppv.actual_start_dt AS actual_start_dte,
                ppv.actual_end_dt AS actual_end_dte,
                COALESCE(ppv.update_dt, ppv.created_dt) AS modified_ts
            FROM paign_placement_version ppv 
            left join paign_default_tactic t 
            ON ppv.tactic_id = t.id
            WHERE COALESCE(ppv.update_dt, ppv.created_dt) > '{latest_ingest_start_ts}'
        )
        SELECT 
            ppv_data.id_version,
            ppv_data.id_tactic,
            ccg.id AS id_content_group,
            ccg.offer_id AS id_offer,
            ppv_data.language_desc,
            ppv_data.audience_segment_desc,
            ppv_data.version_name,
            ppv_data.planned_start_dte,
            ppv_data.planned_end_dte,
            ppv_data.actual_start_dte,
            ppv_data.actual_end_dte,
            ROW_NUMBER() OVER (PARTITION BY aspv.audiencesegment_id ORDER BY cvpp.placement_type_row) AS position_row,
            ROW_NUMBER() OVER (PARTITION BY aspv.audiencesegment_id ORDER BY cvpp.placement_type_column) AS position_column,
            pt.placement_type_name AS placement_type,
            ppv_data.modified_ts
        FROM ppv_data
        LEFT JOIN cf_modules cm ON ppv_data.module_id = cm.id
        LEFT JOIN cf_content_group ccg ON cm.content_group_id = ccg.id
        LEFT JOIN audience_segment_placement_versions aspv ON ppv_data.id_version = aspv.placementversion_id
        LEFT JOIN cf_vehicle_placement_position cvpp ON ppv_data.vehicle_placement_position_id = cvpp.id
        LEFT JOIN cf_placement_type pt ON cvpp.placement_type_id = pt.id
        WHERE ppv_data.id_tactic NOT IN(1267
        ,1271
        ,1277
        ,1280
        ,1285
        ,1270
        ,1279
        ,1274
        ,1272
        ,1265
        ,1261
        ,1264
        ,1262
        ,1275
        ,1259
        ,1283
        ,1281
        ,1273
        ,1284
        ,1263
        ,1282
        ,1286
        ,1266
        ,1278
        ,1260
        ,1269
        ,1268
        ,1276);
    '''

    source_cursor.execute(source_query, (latest_ingest_start_ts,))
    rows = source_cursor.fetchall()
    
    if not rows:
        print("No new or updated records found. No changes made.")
    else:
        # Extract column names
        columns = [desc[0] for desc in source_cursor.description]
        column_names = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(columns))
        
        # Construct update clause dynamically
        update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col != key])

        # Extracting IDs for logging
        record_ids = [str(row[0]) for row in rows]  # row[0] = id_treatment
        record_ids_str = ', '.join(record_ids)  # Convert list to comma-separated string

        # Upsert query with ON CONFLICT
        upsert_query = f'''
            INSERT INTO analytical_model.{table_name} ({column_names})
            VALUES ({placeholders})
            ON CONFLICT ({key}) DO UPDATE SET {update_clause};
        '''

        # Bulk execute upsert query
        dest_cursor.executemany(upsert_query, rows)
        dest_conn.commit()

        # Record end timestamp
        ingest_end_ts = datetime.datetime.utcnow().replace(tzinfo=None)

        # Log ingestion including IDs
        log_query = f'''
            INSERT INTO analytical_model.{update_log_table} ("table", ingest_start_ts, ingest_end_ts, "type", "count", ids)
            VALUES (%s, %s, %s, %s, %s, %s);
        '''
        dest_cursor.execute(log_query, (table_name, ingest_start_ts, ingest_end_ts, "upsert", len(rows), record_ids_str))
        dest_conn.commit()

        print(f"Upserted {len(rows)} records in {table_name}")
        print("Update log recorded.")

    # Close connections
    source_cursor.close()
    source_conn.close()
    dest_cursor.close()
    dest_conn.close()

except Exception as e:
    print("Error:", e)

# COMMAND ----------

# DBTITLE 1,offer
# Define the table to copy data from and to
table_name = "offer"
key = "id_offer"

try:
    # Connect to destination database to get the latest ingest timestamp
    dest_conn = psycopg2.connect(**dest_conn_info)
    dest_cursor = dest_conn.cursor()

    # Fetch latest ingest timestamp for 'version'
    dest_cursor.execute(f'''
        SELECT MAX(ingest_start_ts) FROM analytical_model.{update_log_table}
        WHERE "table" = %s
    ''', (table_name,))
    latest_ingest_start_ts = dest_cursor.fetchone()[0]

    # If no previous ingestion, default to a very old date
    if latest_ingest_start_ts is None:
        latest_ingest_start_ts = datetime.datetime(2000, 1, 1)

    # Record the new start timestamp
    ingest_start_ts = datetime.datetime.utcnow().replace(tzinfo=None)

    # Connect to source database
    source_conn = psycopg2.connect(**source_conn_info)
    source_cursor = source_conn.cursor()

    # Fetch only modified records since last ingest
    source_query = f'''
        SELECT DISTINCT
        o.id AS id_offer,
        o.name || '-' || o.description AS offer_name,
        o.offer_type,
        '' AS hurdle_value,
        '' AS hurdle_type,
        '' AS promo_code,
        value_amount AS award_value,
        value_amount_type AS award_type,
        o.current_status AS status,
        CAST(o.actual_start_dt AS DATE) AS offer_start_dte,
        CAST(o.actual_end_dt AS DATE) AS offer_end_dte, 
        CASE WHEN o.update_dt IS NULL THEN o.created_dt ELSE o.update_dt END AS modified_ts
        FROM paign_default_offer o 
        LEFT JOIN paign_default_valueamounttype vat ON o.value_amount_type_id = vat.id
        WHERE COALESCE(o.update_dt, o.created_dt) > '{latest_ingest_start_ts}'
    '''

    source_cursor.execute(source_query, (latest_ingest_start_ts,))
    rows = source_cursor.fetchall()
    
    if not rows:
        print("No new or updated records found. No changes made.")
    else:
        # Extract column names
        columns = [desc[0] for desc in source_cursor.description]
        column_names = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(columns))
        
        # Construct update clause dynamically
        update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col != key])

        # Extracting IDs for logging
        record_ids = [str(row[0]) for row in rows]  # row[0] = id_treatment
        record_ids_str = ', '.join(record_ids)  # Convert list to comma-separated string

        # Upsert query with ON CONFLICT
        upsert_query = f'''
            INSERT INTO analytical_model.{table_name} ({column_names})
            VALUES ({placeholders})
            ON CONFLICT ({key}) DO UPDATE SET {update_clause};
        '''

        # Bulk execute upsert query
        dest_cursor.executemany(upsert_query, rows)
        dest_conn.commit()

        # Record end timestamp
        ingest_end_ts = datetime.datetime.utcnow().replace(tzinfo=None)

        # Log ingestion including IDs
        log_query = f'''
            INSERT INTO analytical_model.{update_log_table} ("table", ingest_start_ts, ingest_end_ts, "type", "count", ids)
            VALUES (%s, %s, %s, %s, %s, %s);
        '''
        dest_cursor.execute(log_query, (table_name, ingest_start_ts, ingest_end_ts, "upsert", len(rows), record_ids_str))
        dest_conn.commit()

        print(f"Upserted {len(rows)} records in {table_name}")
        print("Update log recorded.")

    # Close connections
    source_cursor.close()
    source_conn.close()
    dest_cursor.close()
    dest_conn.close()

except Exception as e:
    print("Error:", e)

# COMMAND ----------

# DBTITLE 1,link
# Define the table to copy data from and to
table_name = "link"
key = "id_link"

try:
    # Connect to destination database to get the latest ingest timestamp
    dest_conn = psycopg2.connect(**dest_conn_info)
    dest_cursor = dest_conn.cursor()

    # Fetch latest ingest timestamp for 'version'
    dest_cursor.execute(f'''
        SELECT MAX(ingest_start_ts) FROM analytical_model.{update_log_table}
        WHERE "table" = %s
    ''', (table_name,))
    latest_ingest_start_ts = dest_cursor.fetchone()[0]

    # If no previous ingestion, default to a very old date
    if latest_ingest_start_ts is None:
        latest_ingest_start_ts = datetime.datetime(2000, 1, 1)

    # Record the new start timestamp
    ingest_start_ts = datetime.datetime.utcnow().replace(tzinfo=None)

    # Connect to source database
    source_conn = psycopg2.connect(**source_conn_info)
    source_cursor = source_conn.cursor()

    # Fetch only modified records since last ingest
    source_query = f'''
            SELECT DISTINCT
            l.id AS id_link,
            v.id AS id_version,
            split_part(l."original_url", '/', 1) AS domain,
            l.original_url AS base_url,
            l.final_url,
            l.linked_text AS cta_text,
            l.bit_type_name AS link_type,
            l.created_ts AS modified_ts
            FROM paign_module_link_ids_prod l
            JOIN paign_placement_version v
            ON v.tactic_id = l.tactic_id AND l.module_id = v.module_id
            WHERE outdated_flag IS false AND l.created_ts > '{latest_ingest_start_ts}';
        '''

    source_cursor.execute(source_query, (latest_ingest_start_ts,))
    rows = source_cursor.fetchall()
    
    if not rows:
        print("No new or updated records found. No changes made.")
    else:
        # Extract column names
        columns = [desc[0] for desc in source_cursor.description]
        column_names = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(columns))
        
        # Construct update clause dynamically
        update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col != key])

        # Extracting IDs for logging
        record_ids = [str(row[0]) for row in rows]  # row[0] = id_treatment
        record_ids_str = ', '.join(record_ids)  # Convert list to comma-separated string

        # Upsert query with ON CONFLICT
        upsert_query = f'''
            INSERT INTO analytical_model.{table_name} ({column_names})
            VALUES ({placeholders})
            --ON CONFLICT ({key}) DO UPDATE SET {update_clause};
        '''

        # Bulk execute upsert query
        dest_cursor.executemany(upsert_query, rows)
        dest_conn.commit()

        # Record end timestamp
        ingest_end_ts = datetime.datetime.utcnow().replace(tzinfo=None)

        # Log ingestion including IDs
        log_query = f'''
            INSERT INTO analytical_model.{update_log_table} ("table", ingest_start_ts, ingest_end_ts, "type", "count", ids)
            VALUES (%s, %s, %s, %s, %s, %s);
        '''
        dest_cursor.execute(log_query, (table_name, ingest_start_ts, ingest_end_ts, "upsert", len(rows), record_ids_str))
        dest_conn.commit()

        print(f"Upserted {len(rows)} records in {table_name}")
        print("Update log recorded.")

    # Close connections
    source_cursor.close()
    source_conn.close()
    dest_cursor.close()
    dest_conn.close()

except Exception as e:
    print("Error:", e)

# COMMAND ----------

# DBTITLE 1,treatment
# Define the table to copy data from and to
table_name = "treatment"
key = "id_treatment"

dest_schema2 = "public" 

dest_conn = psycopg2.connect(**dest_conn_info)
dest_cursor = dest_conn.cursor()

try:
    # Fetch latest ingest timestamp for 'version'
    dest_cursor.execute(f'''
        SELECT MAX(ingest_start_ts) FROM analytical_model.{update_log_table}
        WHERE "table" = %s
    ''', (table_name,))
    latest_ingest_start_ts = dest_cursor.fetchone()[0]

    # If no previous ingestion, default to a very old date
    if latest_ingest_start_ts is None:
        latest_ingest_start_ts = datetime.datetime(2000, 1, 1)

    # Record the new start timestamp
    ingest_start_ts = datetime.datetime.utcnow().replace(tzinfo=None)

    # Connect to source database
    source_conn = psycopg2.connect(**dest_conn_info)  # Using dest_conn_info
    source_cursor = source_conn.cursor()

    # Fetch only modified records since last ingest
    source_query = f'''
        SELECT DISTINCT
            id,
            treatment_id as id_treatment,
            pv_id as id_version,
            created_at as modified_ts
        FROM {dest_schema2}.treatment_placement_versions
        WHERE created_at > %s;
    '''

    source_cursor.execute(source_query, (latest_ingest_start_ts,))
    rows = source_cursor.fetchall()
    
    if not rows:
        print("No new or updated records found. No changes made.")
    else:
        # Extract column names
        columns = [desc[0] for desc in source_cursor.description]
        column_names = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(columns))
        
        # Construct update clause dynamically
        update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col != key])

        # Extracting IDs for logging
        record_ids = [str(row[0]) for row in rows]  # row[0] = id_treatment
        record_ids_str = ', '.join(record_ids)

        # Upsert query with ON CONFLICT
        upsert_query = f'''
            INSERT INTO analytical_model.{table_name} ({column_names})
            VALUES ({placeholders})
            --ON CONFLICT ({key}) DO UPDATE SET {update_clause};
        '''

        # Bulk execute upsert query
        dest_cursor.executemany(upsert_query, rows)
        dest_conn.commit()

        # Record end timestamp
        ingest_end_ts = datetime.datetime.utcnow().replace(tzinfo=None)

        # Log ingestion including IDs
        log_query = f'''
            INSERT INTO analytical_model.{update_log_table} ("table", ingest_start_ts, ingest_end_ts, "type", "count", ids)
            VALUES (%s, %s, %s, %s, %s, %s);
        '''
        dest_cursor.execute(log_query, (table_name, ingest_start_ts, ingest_end_ts, "upsert", len(rows), record_ids_str))
        dest_conn.commit()

        print(f"Upserted {len(rows)} records in {table_name}")
        print("Update log recorded.")

    # Close connections
    source_cursor.close()
    source_conn.close()
    dest_cursor.close()
    dest_conn.close()

except Exception as e:
    print("Error:", e)


# COMMAND ----------

# DBTITLE 1,define valid data
import psycopg2

def get_valid_ids(dest_conn_info, source_conn_info):
    """
    Retrieves valid campaign, tactic, version, and offer IDs from the appropriate tables.
    Now pulls valid tactics, versions, and offers from the destination DB instead of the source DB.
    """
    valid_campaign_ids, valid_tactic_ids, valid_version_ids, valid_offer_ids = set(), set(), set(), set()

    try:
        # Step 1: Get valid campaign IDs from the destination DB
        with psycopg2.connect(**dest_conn_info) as dest_conn:
            with dest_conn.cursor() as dest_cursor:
                dest_cursor.execute('''
                    SELECT id_campaign FROM analytical_model.validated_campaigns_02282025
                ''')
                valid_campaign_ids = {row[0] for row in dest_cursor.fetchall()}  # Convert to a set

        if not valid_campaign_ids:
            print("No validated campaigns found.")
            return valid_campaign_ids, valid_tactic_ids, valid_version_ids, valid_offer_ids

        # Step 2: Get valid tactic IDs from the destination DB
        with psycopg2.connect(**dest_conn_info) as dest_conn:
            with dest_conn.cursor() as dest_cursor:
                dest_cursor.execute('''
                    SELECT DISTINCT id_tactic FROM analytical_model.tactic
                    WHERE id_campaign = ANY(%s)
                ''', (list(valid_campaign_ids),))
                valid_tactic_ids = {row[0] for row in dest_cursor.fetchall()}  # Convert to a set

                if not valid_tactic_ids:
                    print("No valid tactics found.")
                    return valid_campaign_ids, valid_tactic_ids, valid_version_ids, valid_offer_ids

                # Step 3: Get valid version IDs from the destination DB
                dest_cursor.execute('''
                    SELECT DISTINCT id_version FROM analytical_model.version
                    WHERE id_tactic = ANY(%s)
                ''', (list(valid_tactic_ids),))
                valid_version_ids = {row[0] for row in dest_cursor.fetchall()}  # Convert to a set

                # Step 4: Get valid offer IDs from the version table in the destination DB
                dest_cursor.execute('''
                    SELECT DISTINCT id_offer FROM analytical_model.version
                    WHERE id_tactic = ANY(%s) AND id_offer IS NOT NULL
                ''', (list(valid_tactic_ids),))
                valid_offer_ids = {row[0] for row in dest_cursor.fetchall()}  # Convert to a set

        return valid_campaign_ids, valid_tactic_ids, valid_version_ids, valid_offer_ids

    except Exception as e:
        print("Error fetching valid IDs:", e)
        return valid_campaign_ids, valid_tactic_ids, valid_version_ids, valid_offer_ids



# COMMAND ----------

import psycopg2

def cleanse_invalid_records(dest_conn_info, source_conn_info):
    """
    Removes records from tables in the destination database that do not exist in the valid ID lists.
    """
    try:
        # Fetch valid IDs
        valid_campaign_ids, valid_tactic_ids, valid_version_ids, valid_offer_ids = get_valid_ids(dest_conn_info, source_conn_info)

        # Connect to the destination database
        dest_conn = psycopg2.connect(**dest_conn_info)
        dest_cursor = dest_conn.cursor()

        # Define tables and corresponding valid ID lists
        tables_to_clean = {
            "campaign": ("id_campaign", valid_campaign_ids),
            "link": ("id_version", valid_version_ids),
            "offer": ("id_offer", valid_offer_ids),
            "tactic": ("id_tactic", valid_tactic_ids),
            "version": ("id_version", valid_version_ids),
            "treatment": ("id_version", valid_version_ids)
        }

        # Iterate over tables and remove invalid records
        for table, (column, valid_ids) in tables_to_clean.items():
            if valid_ids:  # Only proceed if there are valid IDs
                query = f'''
                    DELETE FROM analytical_model.{table}
                    WHERE {column} NOT IN %s
                '''
                dest_cursor.execute(query, (tuple(valid_ids),))
                print(f"Cleaned up {table}: Removed records not in valid IDs.")

        # Commit changes and close connection
        dest_conn.commit()
        dest_cursor.close()
        dest_conn.close()
        print("Cleanup process completed successfully.")

    except Exception as e:
        print("Error during cleanup:", e)

# Run the cleanup process
cleanse_invalid_records(dest_conn_info, source_conn_info)
