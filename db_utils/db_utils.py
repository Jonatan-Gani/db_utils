# db_utils.py
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.postgresql import JSON, ARRAY
import json
import glog
import os

# Global variables to hold configuration and engine
_engine = None
_default_db_name = None

# Load configuration at module level
config_path = 'config.json'
with open(config_path, 'r') as f:
    _config = json.load(f)

# Initialize the logger at module level
logger = glog.GLogger(
    backupCount=3,
    is_multiprocessing=False,
    print_logs=True,
    log_dir=_config.get('other', {}).get('log_dir', None)
)
logger.glog("Logger initialized at module level in db_utils")


def init_db_utils(default_db_name):
    """
    Initialize the db_utils module with the default database name.

    :param default_db_name: Default database name to use from the configuration.
    """
    global _config, _engine, _default_db_name

    # Validate default_db_name
    if 'SQL_credentials' not in _config or default_db_name not in _config['SQL_credentials']:
        raise ValueError(f"Database configuration for '{default_db_name}' not found in config file.")

    _default_db_name = default_db_name

    # Create SQLAlchemy engine for the default database
    creds = _config['SQL_credentials'][_default_db_name]
    conn_str = (
        f"postgresql+psycopg2://{creds['DB_USER']}:{creds['DB_PASSWORD']}@"
        f"{creds['DB_HOST']}:{creds['DB_PORT']}/{creds['DB_NAME']}?sslmode=require"
    )
    _engine = create_engine(conn_str)
    logger.glog(f"Database engine created for default database '{_default_db_name}'")


def insert_dataframe(df, table_name, unique_id_col, update=False, db_name=None):
    """
    Inserts a pandas DataFrame into a PostgreSQL database table using SQLAlchemy,
    handling columns with dicts or lists.

    :param df: pandas DataFrame to insert.
    :param table_name: Name of the target table in the database.
    :param unique_id_col: Column name(s) used as unique identifier(s) for upsert operations.
    :param update: If True, perform an upsert (insert or update). If False, insert only.
    :param db_name: Optional database name to use instead of the default.
    """
    global _config, _engine, _default_db_name

    if _config is None or _default_db_name is None:
        raise RuntimeError("db_utils is not initialized. Please call init_db_utils() before using this function.")

    # Determine which database to use
    if db_name is None:
        db_name = _default_db_name
        engine = _engine
        logger.glog(f"Using default database '{db_name}'")
    else:
        if db_name not in _config['SQL_credentials']:
            raise ValueError(f"Database configuration for '{db_name}' not found in config file.")
        # Create a one-time engine for the specified database
        creds = _config['SQL_credentials'][db_name]
        conn_str = (
            f"postgresql+psycopg2://{creds['DB_USER']}:{creds['DB_PASSWORD']}@"
            f"{creds['DB_HOST']}:{creds['DB_PORT']}/{creds['DB_NAME']}?sslmode=require"
        )
        engine = create_engine(conn_str)
        logger.glog(f"Created SQLAlchemy engine for database '{db_name}'")

    logger.glog("Starting insert_dataframe function")

    # Use a connection context manager
    with engine.connect() as connection:
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
                transaction.rollback()
                return

            # Verify that DataFrame columns match the database table columns
            db_columns = table.columns.keys()
            logger.glog(f"Database table columns: {db_columns}")

            missing_cols = set(df.columns) - set(db_columns)
            if missing_cols:
                logger.glog(
                    f"Warning: The following columns are not in the database table '{table_name}': {missing_cols}"
                )

            # Reorder DataFrame columns to match the database table
            df = df[[col for col in db_columns if col in df.columns]]
            logger.glog(f"DataFrame columns reordered to match database table: {df.columns.tolist()}")

            # Prepare data types for columns with dicts or lists
            for column in table.columns:
                col_name = column.name
                if col_name in df.columns:
                    logger.glog(f"Processing column '{col_name}' with type '{column.type}'")
                    if isinstance(column.type, JSON):
                        # Ensure dicts/lists are serialized as JSON
                        df[col_name] = df[col_name].apply(
                            lambda x: x if x is None else json.dumps(x)
                        )
                        logger.glog(f"Serialized column '{col_name}' as JSON")
                    elif isinstance(column.type, ARRAY):
                        # For arrays, SQLAlchemy handles it
                        logger.glog(f"Handled column '{col_name}' as ARRAY type")

            # Convert DataFrame to list of dictionaries
            data = df.to_dict(orient='records')
            logger.glog(f"Prepared data for insertion: {data}")

            # Insert or update data
            if update:
                # Perform an upsert (insert or update)
                logger.glog("Performing upsert operation")
                stmt = pg_insert(table).values(data)
                if isinstance(unique_id_col, list):
                    index_elements = unique_id_col
                else:
                    index_elements = [unique_id_col]
                update_dict = {c.name: c for c in stmt.excluded if c.name not in index_elements}
                stmt = stmt.on_conflict_do_update(
                    index_elements=index_elements,
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
            transaction.rollback()
            logger.glog(f"An error occurred during database operation: {e}")

        finally:
            logger.glog("Database connection closed")
