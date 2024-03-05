-- Example: Transform SAP BYD JSON in Airbyte

-- Assuming you have a table with a JSON column
CREATE TABLE sap_byd_data (
    id INT,
    json_data JSONB
);

-- Extracting and transforming relevant data from the JSON dynamically
SELECT
    id,
    kv.key AS property_name,
    kv.value AS property_value
FROM
    sap_byd_data,
    jsonb_each_text(json_data) AS kv;
