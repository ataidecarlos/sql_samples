DROP PROCEDURE IF EXISTS dbo.usp_delete_table_partition
GO

IF OBJECT_ID('tempDB..#partition_ids', 'U') IS NOT NULL
DROP TABLE #partition_ids
GO

CREATE TABLE #partition_ids ( batch_id int );
GO

CREATE PROCEDURE dbo.usp_delete_table_partition
    @batch_id int = 0
AS
BEGIN

declare @internal_ids table ( batch_id int, partition_number int default 0, partition_exists bit default 0, completed bit default 0)
declare @partition_number int

-- WARNING: This is a proof-of-concept. Do not use this code in any production environment without extensive testing and/or re-writing the code.
--
-- Description
-- Receives either a temporary table or variable with batch_id to be switched and deleted.
-- This will only be done for the partitions that exist.
--
-- At this stage, there should be 0 rows in the partition that we want to delete.




-- Copy the batch_id from the temporary table or variable to the new table variable
insert into @internal_ids (batch_id) select @batch_id

if (object_id('tempDB..#partition_ids', 'U') is not null)
begin
    insert into @internal_ids (batch_id)
    select batch_id from #partition_ids
    where batch_id != @batch_id
end


-- Set the flag for existing partitions (and validate the batch_id)
update @internal_ids set partition_exists = 1
    , partition_number = p.partition_number
from sys.partitions p
join sys.indexes i on p.object_id = i.object_id and p.index_id = i.index_id
join sys.partition_schemes ps on ps.data_space_id = i.data_space_id
join sys.partition_functions pf on pf.function_id = ps.function_id
left join sys.partition_range_values prv on prv.function_id = pf.function_id 
    and prv.boundary_id = p.partition_number - 1
where i.object_id = OBJECT_ID('user_data')
    and i.index_id <= 1
    and batch_id = prv.value
    --and batch_id <> 0


-- Switch the partition and delete it
while exists (select 1 from @internal_ids where completed = 0 and partition_exists = 1)
begin
    select @partition_number = partition_number
        , @batch_id = batch_id
    from @internal_ids
    where completed = 0
        and partition_exists = 1

    alter table user_data switch partition @partition_number to user_data_cleanup partition @partition_number        
    select 'Switched partition id ' + convert(varchar(10),@partition_number) + ' | batch_id ' + convert(varchar(10),@batch_id) as 'Result'

    alter partition function pf_batch_id() merge range(@batch_id)
    select 'Merged range (batch_id) ' + convert(varchar(10),@batch_id) as 'Result'

    -- Truncate the target table AFTER the switch
    truncate table user_data_cleanup

    --  Mark the partition as completed
    update @internal_ids set completed = 1 where batch_id = @batch_id and partition_number = @partition_number


    -- Partition number is dynamic, so we need to update it after each partition switch
    update @internal_ids set partition_number = p.partition_number
    from sys.partitions p
    join sys.indexes i on p.object_id = i.object_id and p.index_id = i.index_id
    join sys.partition_schemes ps on ps.data_space_id = i.data_space_id
    join sys.partition_functions pf on pf.function_id = ps.function_id
    left join sys.partition_range_values prv on prv.function_id = pf.function_id 
        and prv.boundary_id = p.partition_number - 1
    where i.object_id = OBJECT_ID('user_data')
        and i.index_id <= 1
        and batch_id = prv.value
        --and batch_id <> 0

end


END
GO
