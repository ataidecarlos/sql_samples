DROP PROCEDURE IF EXISTS dbo.usp_create_partition_bulk
GO

IF OBJECT_ID('tempDB..#partition_ids', 'U') IS NOT NULL
DROP TABLE #partition_ids
GO

CREATE TABLE #partition_ids ( batch_id int );
GO

CREATE PROCEDURE dbo.usp_create_partition_bulk
    @batch_id int = 0
AS
BEGIN

declare @internal_ids table ( batch_id int, is_new bit default 1 )
declare @id int
    

-- WARNING: This is a proof-of-concept. Do not use this code in any production environment without extensive testing and/or re-writing the code.
--
-- Description
--
-- Receives either a temporary table or variable with batch_id to be created.
-- Check if the ids already exist in the partition function.
-- If yes, no action, else it creates a new partition for that id.


-- Copy the batch_id from the temporary table or variable to the new table variable
insert into @internal_ids (batch_id) select @batch_id

if (object_id('tempDB..#partition_ids', 'U') is not null)
begin
    insert into @internal_ids (batch_id)
    select batch_id from #partition_ids
    where batch_id != @batch_id
end



update @internal_ids set is_new = 0
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


if (select count(is_new) from @internal_ids where is_new = 1) > 0
begin

    declare cursor_batch_id cursor for
    select batch_id from @internal_ids where is_new = 1

    open cursor_batch_id
    fetch next from cursor_batch_id into @id

    while @@FETCH_STATUS = 0
    begin

        alter partition scheme ps_batch_id next used [primary]
        alter partition function pf_batch_id() split range(@id)
        
        select 'Created batch_id ' + convert(varchar(10),@id) as 'Result'

        fetch next from cursor_batch_id into @id
    end

    close cursor_batch_id
    deallocate cursor_batch_id
    
end


END
GO

IF OBJECT_ID('tempDB..#partition_ids', 'U') IS NOT NULL
DROP TABLE #partition_ids
GO

-- Example to execute the stored procedure we just created
-- EXECUTE dbo.usp_create_partition_bulk 1
-- GO