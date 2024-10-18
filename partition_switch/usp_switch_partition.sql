DROP PROCEDURE IF EXISTS dbo.usp_switch_partition
GO

CREATE PROCEDURE dbo.usp_switch_partition
    @source_table_name sysname = 'EMPTY',
    @target_table_name sysname = 'EMPTY',
    @batch_id int = 0
AS
BEGIN

declare
    @sql_dynamic nvarchar(1000) = '',
    @partition_function nvarchar(255),
    @partition_scheme nvarchar(255),
    @partition_number int

-- WARNING: This is a proof-of-concept. Do not use this code in any production environment without extensive testing and/or re-writing the code.
--
-- Description
-- Target table with the same structure as the source table, so that a SWITCH PARTITION command can be executed.
-- Target table is also being truncated before and after the SWITCH PARTITION command, to ensure all operations are as fast as possible and minimally logged.



if (@source_table_name = 'EMPTY' or @target_table_name = 'EMPTY' or @batch_id = 0)
begin
    select 'stopping - received NULL values in parameters' as 'Result'
    return
end

-- TODO
-- Add more checks to ensure the source and target tables are using the same partition function and partition scheme


-- Get the partition number for the batch_id
select
    @partition_number = p.partition_number
from sys.partitions p
join sys.indexes i on p.object_id = i.object_id and p.index_id = i.index_id
join sys.partition_schemes ps on ps.data_space_id = i.data_space_id
join sys.partition_functions pf on pf.function_id = ps.function_id
left join sys.partition_range_values prv on prv.function_id = pf.function_id 
    and prv.boundary_id = p.partition_number - 1 -- the first boundary_id will be 1, but there is a partition before batch_id 0, for values to the "left" of 0.
where i.object_id = OBJECT_ID(@source_table_name)
    and i.index_id <= 1
    and prv.[value] = @batch_id
order by p.partition_number

if (@@rowcount > 0)
begin

    -- Truncate the target table before the switch
    -- Is it necessary? With proper error checking on the outcome of the stored procedure, Truncate can be executed only at the end.
    set @sql_dynamic = 'truncate table ' + @target_table_name +' ;'
    execute sp_executesql @sql_dynamic

    set @sql_dynamic = 'alter table ' + @source_table_name 
    set @sql_dynamic = @sql_dynamic + ' switch partition ' + convert(nvarchar(10), @partition_number)
    set @sql_dynamic = @sql_dynamic + ' to ' + @target_table_name
    set @sql_dynamic = @sql_dynamic + ' partition ' + convert(nvarchar(10), @partition_number) + ' ;'
    execute sp_executesql @sql_dynamic

    -- Truncate the target table after the switch
    set @sql_dynamic = 'truncate table ' + @target_table_name +' ;'
    execute sp_executesql @sql_dynamic

end



END
GO
-- example to execute the stored procedure we just created
-- EXECUTE dbo.usp_switch_partition 'user_data', 'user_data_cleanup', 1
-- GO