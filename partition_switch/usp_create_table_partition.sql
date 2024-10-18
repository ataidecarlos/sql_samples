DROP PROCEDURE IF EXISTS dbo.usp_create_table_partition
GO

CREATE PROCEDURE dbo.usp_create_table_partition
    @table_name sysname = 'EMPTY',
    @batch_id int = 0
AS
BEGIN

declare
    @sql_dynamic nvarchar(1000) = '',
    @partition_function nvarchar(255),
    @partition_scheme nvarchar(255)
    

-- WARNING: This is a proof-of-concept. Do not use this code in any production environment without extensive testing and/or re-writing the code.
--
-- Description
--
-- Given a table name and batch_id, we check if a partition for that value already exists.
-- If yes, no action. If no, we create it.


if (@table_name = 'EMPTY' or @batch_id = 0)
begin
    select 'stopping - received NULL values in parameters' as 'Result'
    return
end

-- TODO
-- Should all variables be converted into nvarchar automatically just for readability in dynamic SQL?

select
    @partition_function = pf.name,
    @partition_scheme = ps.name
from sys.partitions p
join sys.indexes i on p.object_id = i.object_id and p.index_id = i.index_id
join sys.partition_schemes ps on ps.data_space_id = i.data_space_id
join sys.partition_functions pf on pf.function_id = ps.function_id
left join sys.partition_range_values prv on prv.function_id = pf.function_id 
    and prv.boundary_id = p.partition_number - 1
where i.object_id = OBJECT_ID(@table_name)
    and i.index_id <= 1
    --
    and prv.[value] = @batch_id
order by p.partition_number

if (@@rowcount = 0)
begin

    select
        @partition_function = pf.name,
        @partition_scheme = ps.name
    from sys.partitions p
    join sys.indexes i on p.object_id = i.object_id and p.index_id = i.index_id
    join sys.partition_schemes ps on ps.data_space_id = i.data_space_id
    join sys.partition_functions pf on pf.function_id = ps.function_id
    where i.object_id = OBJECT_ID(@table_name)
        and i.index_id <= 1

    set @sql_dynamic = 'alter partition scheme ' + @partition_scheme + ' next used [primary];'
    set @sql_dynamic = @sql_dynamic + 'alter partition function ' + @partition_function + '() split range(' + convert(nvarchar(10), @batch_id) + ')'
    execute sp_executesql @sql_dynamic

    select 'created batch_id ' + convert(nvarchar(10),@batch_id) as 'Result'
end
else
begin
    select 'batch_id ' + convert(nvarchar(10),@batch_id) + ' already exists' as 'Result'
end

END
GO

-- Example to execute the stored procedure we just created
-- EXECUTE dbo.usp_create_table_partition 'user_data', 1
-- GO