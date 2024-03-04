# Sample Transformation Script for SAP BYD OData to Snowflake

import requests
import snowflake.connector

# SAP BYD OData API endpoint
sap_byd_api_url = "https://my350410.sapbydesign.com"

# Snowflake connection parameters
snowflake_connection_params = {
    "account": "https://sefnlwg-sj89076.snowflakecomputing.com",
    "user": "imran123",
    "password": "Immu@1143",
    "warehouse": "COMPUTE_WH",
    "database": "TESTBYD",
    "schema": "MMM",
}

# Function to fetch data from SAP BYD OData
def fetch_sap_byd_data():
    # Your logic to authenticate and fetch data from SAP BYD OData
    # Example using requests library:
    response = requests.get(sap_byd_api_url + "/sap/byd/odata/analytics/ds/Material.svc/Material?$format=json&$inlinecount=allpages")
    return response.json()["d"]["results"]

# Function to transform and load data into Snowflake
def load_data_into_snowflake(data):
    # Establish Snowflake connection
    connection = snowflake.connector.connect(**snowflake_connection_params)
    cursor = connection.cursor()

    # Your logic to transform and load data into Snowflake
    # Example: Loop through data and insert into a Snowflake table
    for record in data:
        cursor.execute(
            f"INSERT INTO your_snowflake_table (column1, column2, ...) VALUES (%s, %s, ...)",
            (record["property1"], record["property2"], ...)
        )

    # Commit changes and close connection
    cursor.execute("COMMIT")
    cursor.close()
    connection.close()

# Main function to orchestrate the ETL process
def main():
    # Extract data from SAP BYD OData
    sap_byd_data = fetch_sap_byd_data()

    # Transform and load data into Snowflake
    load_data_into_snowflake(sap_byd_data)

if __name__ == "__main__":
    main()
