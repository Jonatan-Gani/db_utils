# db_utils.py
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, inspect, Column, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.postgresql import JSON, ARRAY
import json
import glog


config_path = 'config.json'
with open(config_path, 'r') as f:
    config = json.load(f)

logger = glog.GLogger(backupCount=3,
                      is_multiprocessing=False,
                      print_logs=True,
                      log_dir=config['project_vars']['log_dir'])


def insert_dataframe(df, table_name, unique_id_col, update=False, creds=config['SQL_credentials']):
    logger.glog("Start insert_dataframe function")
    """
    Inserts a pandas DataFrame into a PostgreSQL database table using SQLAlchemy,
    handling columns with dicts or lists.
    """

    # Create SQLAlchemy engine
    conn_str = f"postgresql+psycopg2://{creds['DB_USER']}:{creds['DB_PASSWORD']}@{creds['DB_HOST']}:{creds['DB_PORT']}/{creds['DB_NAME']}?sslmode=require"

    engine = create_engine(conn_str)
    logger.glog("Created SQLAlchemy engine")

    connection = engine.connect()
    logger.glog("Established connection to the database")

    # Start transaction
    transaction = connection.begin()
    try:
        metadata = MetaData()
        schema = table_name.split('.')[0] if '.' in table_name else None
        tbl_name = table_name.split('.')[-1]
        logger.glog(f"Schema: {schema}, Table name: {tbl_name}")

        # Reflect the table from the database
        try:
            table = Table(tbl_name, metadata, autoload_with=engine, schema=schema)
            logger.glog(f"Reflected table '{tbl_name}' successfully")
        except Exception as e:
            logger.glog(f"Error reflecting table '{tbl_name}': {e}")
            transaction.rollback()  # Rollback the transaction
            connection.close()
            engine.dispose()
            return

        # Verify that DataFrame columns match the database table columns
        db_columns = table.columns.keys()
        logger.glog(f"Database table columns: {db_columns}")

        missing_cols = set(df.columns) - set(db_columns)
        if missing_cols:
            logger.glog(f"Warning: The following columns are not in the database table '{table_name}': {missing_cols}")

        # Reorder DataFrame columns to match the database table
        df = df[[col for col in db_columns if col in df.columns]]
        logger.glog(f"DataFrame columns reordered to match database table: {df.columns.tolist()}")

        # Prepare data types for columns with dicts or lists
        type_mappings = {}
        for column in table.columns:
            col_name = column.name
            if col_name in df.columns:
                logger.glog(f"Processing column '{col_name}' with type '{column.type}'")
                if isinstance(column.type, JSON):
                    # Ensure dicts/lists are serialized as JSON
                    df[col_name] = df[col_name].apply(lambda x: x if x is None else json.dumps(x))
                    type_mappings[col_name] = JSON
                    logger.glog(f"Serialized column '{col_name}' as JSON")
                elif isinstance(column.type, ARRAY):
                    # For arrays, no need to serialize; SQLAlchemy handles it
                    type_mappings[col_name] = ARRAY(column.type.item_type)
                    logger.glog(f"Handled column '{col_name}' as ARRAY type")
                else:
                    # Map other types as necessary
                    pass  # SQLAlchemy handles basic types automatically

        # Convert DataFrame to list of dictionaries
        data = df.to_dict(orient='records')
        logger.glog(f"Prepared data for insertion: {data}")

        # Insert or update data
        if update:
            # Perform an upsert (insert or update)
            logger.glog("Performing upsert operation")
            stmt = pg_insert(table).values(data)
            update_dict = {c.name: c for c in stmt.excluded if c.name != unique_id_col}
            stmt = stmt.on_conflict_do_update(
                index_elements=[unique_id_col],
                set_=update_dict
            )
            result = connection.execute(stmt)
            logger.glog(f"Upsert operation result: {result.rowcount} rows affected")
        else:
            # Insert data
            logger.glog("Performing insert operation")
            result = connection.execute(table.insert(), data)
            logger.glog(f"Insert operation result: {result.rowcount} rows inserted")

        # Commit the transaction
        transaction.commit()
        logger.glog("Transaction committed successfully")

    except Exception as e:
        transaction.rollback()  # Rollback in case of an error
        logger.glog(f"An error occurred during database operation: {e}")
    finally:
        connection.close()
        engine.dispose()
        logger.glog("Database connection closed")

