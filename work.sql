
-- Get the partition function, partition number, boundary value, and row count for each partition in the given table
SELECT TOP 100
    pf.name AS partition_function,
    pf.boundary_value_on_right,
    prv.value AS boundary_value,
    p.rows AS row_count
FROM sys.partitions p
JOIN sys.indexes i ON p.object_id = i.object_id AND p.index_id = i.index_id
JOIN sys.partition_schemes ps ON ps.data_space_id = i.data_space_id
JOIN sys.partition_functions pf ON pf.function_id = ps.function_id
LEFT JOIN sys.partition_range_values prv ON prv.function_id = pf.function_id 
    AND prv.boundary_id = p.partition_number - 1 -- In this case, the first boundary_id will be 0, but there is a partition before 0, for values to the "left" of 0.
WHERE i.object_id = OBJECT_ID('user_data')
    AND i.index_id <= 1
    AND prv.function_id IS NOT NULL
    --AND p.rows > 0
    --AND prv.value >= 1000
ORDER BY p.partition_number;

-- More expensive than the previous query, but it returns data from the user table, making it easier to visualize the values/partition numbers
SELECT
    max(batch_id) as batch_id,
    $PARTITION.pf_batch_id(batch_id) as partition_number,
    count(batch_id) as row_count
FROM user_data
WHERE batch_id = 33
GROUP BY $PARTITION.pf_batch_id(batch_id)



-- Create an aligned index on the batch_id column
CREATE INDEX idx_batch_id ON user_data(batch_id) on ps_batch_id (batch_id)


-- Samples
EXECUTE dbo.usp_create_table_partition 'user_data', 1;
EXECUTE dbo.usp_create_table_partition 'user_data', 2;
EXECUTE dbo.usp_create_table_partition 'user_data', 3;

EXECUTE dbo.usp_switch_partition 'user_data', 'user_data_cleanup', 1;

EXECUTE dbo.usp_delete_table_partition 'user_data', 1;
