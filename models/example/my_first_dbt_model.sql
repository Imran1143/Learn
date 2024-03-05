-- Example: Transform SAP BYD JSON in Airbyte

-- Assuming you have a table with a JSON column
CREATE TABLE sap_byd_data (
    id INT,
    json_data JSONB
);

-- Extracting and transforming relevant data from the JSON
SELECT
    id,
    json_data->>'propertyName' AS property_name,
    json_data->>'anotherProperty' AS another_property,
    -- Add more transformations as needed
FROM
    sap_byd_data;
