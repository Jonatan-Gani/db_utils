### README for `db_utils.py`

---

# **`db_utils` Module**

The `db_utils.py` module provides utility functions for interacting with PostgreSQL databases using SQLAlchemy and pandas. It is designed to simplify database operations such as data insertion, querying, and management of connections in a configurable and extensible way.

---

## **Features**

- **Database Initialization**: Initializes a SQLAlchemy engine with a specified default database, based on configurations in a configuration file.
- **Data Insertion and Upsertion**: Supports inserting pandas DataFrames into PostgreSQL tables, with optional upsert functionality.
- **Instrument Identification**: Fetches instrument identifiers from the `instrument_identifiers` table, supporting cross-database queries.
- **Dynamic Database Switching**: Allows operations across multiple databases without reinitialization.
- **Structured Logging**: Integrated logging using the `glog` library for detailed operation tracking and debugging.

---

## **Configuration**

The module relies on a configuration file for database credentials and logging options. The file should include details such as database host, port, username, password, and name, along with other optional configurations like logging directories.

---

## **Usage**

1. **Initialization**: Before using any functions, initialize the module with the default database name. This sets up the database engine and validates configurations.
2. **Data Insertion**: Use the `insert_dataframe` function to insert or upsert data into a specified PostgreSQL table. Supports handling of JSON and array data types.
3. **Identifier Retrieval**: Use the `get_instrument_identifier` function to fetch specific identifiers from the `instrument_identifiers` table, with support for flexible querying options.
4. **Logging**: All database interactions and potential errors are logged to the specified logging directory, enabling easy debugging and monitoring.

---

## **Key Functions**

- **Initialization**: Set up the default database for subsequent operations.
- **Insert DataFrame**: Inserts pandas DataFrames into PostgreSQL tables, with support for schema reflection, column validation, and optional conflict handling for updates.
- **Retrieve Identifiers**: Fetches data from the `instrument_identifiers` table based on customizable parameters.
- **Dynamic Database Operations**: Allows the use of multiple databases within a single session by dynamically switching configurations.

---

## **Best Practices**

- Ensure the configuration file is secure and contains accurate credentials.
- Initialize the module before performing any database operations.
- Use structured logging to monitor and debug issues effectively.
- Validate dataframes before insertion to avoid schema mismatches.

---

## **Dependencies**

- Python 3.8+
- pandas
- SQLAlchemy
- psycopg2
- glog

---

## **Limitations**

- Designed specifically for PostgreSQL databases.
- Relies on a pre-configured `config.json` file for database credentials.
- JSON and array handling depend on proper table schema configuration.

---

## **Support**

For issues or feature requests, please contact the developer or submit a pull request or issue on the repository where this module is hosted.