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


def get_instrument_identifier(
        identifier_value,
        identifier_type="instrument_id",
        requested_identifier_type=None,
        db_name=None
        ):
    """
    Retrieve an identifier from the instrument_identifiers table.

    :param identifier_value: Value of the identifier to search for.
    :param identifier_type: Type of identifier to search by (default is 'instrument_id').
    :param requested_identifier_type: Type of identifier to retrieve (default is the same as identifier_type).
    :param db_name: Optional database name to use instead of the default.
    :return: The requested identifier value or raises an error if not found or an issue occurs.
    """
    global _engine, _config, _default_db_name

    # Ensure db_utils is initialized
    if _engine is None or _default_db_name is None:
        raise RuntimeError("db_utils is not initialized. Please call init_db_utils() before using this function.")

    # Validate parameters
    if not identifier_value or not identifier_type:
        raise ValueError("Both identifier_value and identifier_type must be provided.")
    if requested_identifier_type is None:
        requested_identifier_type = identifier_type

    logger.glog("Starting get_instrument_identifier function")

    # Determine which database to use
    if db_name is None:

        db_name = _default_db_name
        engine = _engine
        logger.glog(f"Using  default database '{db_name}'")
    else:
        if db_name not in _config['SQL_credentials']:
            raise ValueError(f"Database configuration for '{db_name}' not found in config file.")
        creds = _config['SQL_credentials'][db_name]
        conn_str = (
            f"postgresql+psycopg2://{creds['DB_USER']}:{creds['DB_PASSWORD']}@"
            f"{creds['DB_HOST']}:{creds['DB_PORT']}/{creds['DB_NAME']}?sslmode=require"
        )
        engine = create_engine(conn_str)
        logger.glog(f"Created SQLAlchemy engine for database '{db_name}'")

    try:
        with engine.connect() as connection:
            logger.glog("Established connection to the database")
            metadata = MetaData()
            instrument_identifiers = Table(
                "instrument_identifiers",
                metadata,
                autoload_with=engine,
                schema="public"
            )
            logger.glog("Reflected instrument_identifiers table successfully")

            # Build the query
            query = (
                instrument_identifiers.select()
                .where(instrument_identifiers.c[identifier_type] == identifier_value)
            )

            logger.glog(f"Querying {identifier_type} with value '{identifier_value}'")
            result = connection.execute(query).fetchall()

            # Handle query results
            if not result:
                raise LookupError(f"No entries found for {identifier_type} = {identifier_value}")
            elif len(result) > 1:
                raise ValueError(f"Duplicate entries found for {identifier_type} = {identifier_value}")

            logger.glog(f"Query successful: {result}")

            # Retrieve the requested identifier type
            identifier_data = result[0]
            if requested_identifier_type not in identifier_data.keys():
                raise ValueError(
                    f"Requested identifier type '{requested_identifier_type}' does not exist in the table.")

            return identifier_data[requested_identifier_type]

    except LookupError as e:
        logger.glog(f"Lookup error: {e}")
        raise
    except ValueError as e:
        logger.glog(f"Value error: {e}")
        raise
    except Exception as e:
        logger.glog(f"General error during database operation: {e}")
        raise
    finally:
        logger.glog("Database connection closed")


def get_instrument_id(
        input_value,
        input_type="instrument_id",
        output_type=None,
        return_all=False,
        db_name=None
        ):
    """
    Flexible lookup function for instrument identifiers.

    Retrieves instrument identifiers from the instrument_identifiers table using any identifier
    type to find any other identifier type. Always returns structured results in JSON format for context.

    :param input_value: Value of the identifier to search by
    :param input_type: Type of identifier to search by (default is 'instrument_id')
    :param output_type: Type of identifier to retrieve (if None, returns all types for that instrument)
    :param return_all: If True, returns all matching rows. If False, returns only the first match.
    :param db_name: Optional database name to use instead of the default.
    :return:
        - If return_all=False: A dict with the result and context information
        - If return_all=True: A list of dicts with results and context information
    :raises: LookupError if no matches found, ValueError for invalid inputs
    """
    global _engine, _config, _default_db_name

    # Ensure db_utils is initialized
    if _engine is None or _default_db_name is None:
        raise RuntimeError("db_utils is not initialized. Please call init_db_utils() before using this function.")

    # Validate parameters
    if not input_value:
        raise ValueError("input_value must be provided")

    logger.glog(
        f"Starting lookup_instrument_identifiers: input_value={input_value}, input_type={input_type}, output_type={output_type}")

    # Determine which database to use
    if db_name is None:
        db_name = _default_db_name
        engine = _engine
        logger.glog(f"Using default database '{db_name}'")
    else:
        if db_name not in _config['SQL_credentials']:
            raise ValueError(f"Database configuration for '{db_name}' not found in config file.")
        creds = _config['SQL_credentials'][db_name]
        conn_str = (
            f"postgresql+psycopg2://{creds['DB_USER']}:{creds['DB_PASSWORD']}@"
            f"{creds['DB_HOST']}:{creds['DB_PORT']}/{creds['DB_NAME']}?sslmode=require"
        )
        engine = create_engine(conn_str)
        logger.glog(f"Created SQLAlchemy engine for database '{db_name}'")

    try:
        with engine.connect() as connection:
            logger.glog("Established connection to the database")
            metadata = MetaData()

            # Reflect the tables
            instrument_identifiers = Table(
                "instrument_identifiers",
                metadata,
                autoload_with=engine,
                schema="public"
            )
            logger.glog("Reflected instrument_identifiers table")

            # Build the query based on input type
            if input_type == "instrument_id":
                # Direct lookup by instrument_id
                query = (
                    instrument_identifiers.select()
                    .where(instrument_identifiers.c.instrument_id == input_value)
                )
                logger.glog(f"Querying by instrument_id={input_value}")
            else:
                # Look up rows where identifier_type matches input_type and identifier_value matches input_value
                query = (
                    instrument_identifiers.select()
                    .where(
                        (instrument_identifiers.c.identifier_type == input_type) &
                        (instrument_identifiers.c.identifier_value == input_value)
                    )
                )
                logger.glog(f"Querying by identifier_type={input_type} and identifier_value={input_value}")

            # Execute the query
            result = connection.execute(query).fetchall()

            if not result:
                raise LookupError(f"No entries found for {input_type}={input_value}")

            logger.glog(f"Found {len(result)} matching records")

            # Get all instrument IDs from the initial query
            instrument_ids = [row.instrument_id for row in result]

            # If looking for a specific output_type
            if output_type:
                if output_type == "instrument_id":
                    # Format instrument_id results with context
                    output_results = []
                    for row in result:
                        output_results.append({
                            "instrument_id": row.instrument_id,
                            "id": row.instrument_id,  # The actual value
                            "id_type": "instrument_id",
                            "input_value": input_value,
                            "input_type": input_type
                        })
                else:
                    # Query for the specific output_type
                    output_query = (
                        instrument_identifiers.select()
                        .where(
                            (instrument_identifiers.c.instrument_id.in_(instrument_ids)) &
                            (instrument_identifiers.c.identifier_type == output_type)
                        )
                    )

                    output_result = connection.execute(output_query).fetchall()

                    if not output_result:
                        raise LookupError(f"No {output_type} found for {input_type}={input_value}")

                    # Format results with context
                    output_results = []
                    for row in output_result:
                        output_results.append({
                            "instrument_id": row.instrument_id,
                            "id": row.identifier_value,  # The actual value
                            "id_type": output_type,
                            "input_value": input_value,
                            "input_type": input_type
                        })
            else:
                # Query all identifier types for these instrument IDs
                all_ids_query = (
                    instrument_identifiers.select()
                    .where(instrument_identifiers.c.instrument_id.in_(instrument_ids))
                )

                all_results = connection.execute(all_ids_query).fetchall()

                # Group by instrument_id
                grouped_results = {}
                for row in all_results:
                    if row.instrument_id not in grouped_results:
                        grouped_results[row.instrument_id] = {
                            "instrument_id": row.instrument_id,
                            "identifiers": [],
                            "input_value": input_value,
                            "input_type": input_type
                        }

                    grouped_results[row.instrument_id]["identifiers"].append({
                        "id": row.identifier_value,
                        "id_type": row.identifier_type
                    })

                # Format into a list
                output_results = list(grouped_results.values())

            # Return as a list or single item
            if return_all:
                logger.glog(f"Returning list of {len(output_results)} results")
                return output_results
            else:
                logger.glog(f"Returning single result")
                return output_results[0]

    except LookupError as e:
        logger.glog(f"Lookup error: {e}")
        raise
    except ValueError as e:
        logger.glog(f"Value error: {e}")
        raise
    except Exception as e:
        logger.glog(f"General error during database operation: {e}")
        raise
    finally:
        logger.glog("Database connection closed")