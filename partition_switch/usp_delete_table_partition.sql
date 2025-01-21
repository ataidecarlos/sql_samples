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

declare @internal_ids table ( batch_id int, partition_exists bit default 0 )

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


-- Check if the partition exists
update @internal_ids set partition_exists = 1
where batch_id in (
    select prv.[value]
    from sys.partitions p
    join sys.indexes i on p.object_id = i.object_id and p.index_id = i.index_id
    join sys.partition_schemes ps on ps.data_space_id = i.data_space_id
    join sys.partition_functions pf on pf.function_id = ps.function_id
    left join sys.partition_range_values prv on prv.function_id = pf.function_id 
        and prv.boundary_id = p.partition_number - 1
    where i.object_id = OBJECT_ID('user_data')
        and i.index_id <= 1
    )


-- Switch the partition and delete it
if (select count(partition_exists) from @internal_ids where partition_exists = 1) > 0
begin

    -- Truncate the target table BEFORE the switch
    truncate table user_data


    declare cursor_batch_id cursor for
    select batch_id from @internal_ids where partition_exists = 1

    open cursor_batch_id
    fetch next from cursor_batch_id into @id

    while @@FETCH_STATUS = 0
    begin

        alter table user_data switch partition @id to user_data_cleanup partition @id        
        select 'Switched batch_id ' + convert(varchar(10),@id) as 'Result'

        alter partition function pf_batch_id() merge range(@id)
        select 'Deleted partition batch_id ' + convert(varchar(10),@id) as 'Result'

        fetch next from cursor_batch_id into @id
    end

    close cursor_batch_id
    deallocate cursor_batch_id


    -- Truncate the target table AFTER the switch
    truncate table user_data

end



END
GO

-- EXECUTE dbo.usp_delete_table_partition 'user_data', 1
-- GO