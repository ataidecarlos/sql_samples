
-- Create the partition function and scheme
-- For this example, we know the partition column is an integer with 4 digits
-- The values are between 0 and 9999 just for the sake of the example
create partition function pf_batch_id (int) as range right for values (0,9999);
create partition scheme ps_batch_id as partition pf_batch_id all to ([primary]);

-- Cleanup
-- drop partition scheme ps_batch_id;
-- drop partition function pf_batch_id;

-- Main table that holds user data and is now being partitioned by batch_id
drop table if exists user_data;
create table user_data (
    [batch_id] [int] NOT NULL,
    [col_int_1] [int] NOT NULL,
    [col_int_2] [int] NOT NULL,
    [col_int_3] [int] NOT NULL,
    [col_float_1] [decimal](19, 5) NOT NULL,
    [col_float_2] [decimal](19, 5) NOT NULL,
    [col_float_3] [decimal](19, 5) NOT NULL,
    [col_float_4] [decimal](19, 5) NOT NULL,
    [col_float_5] [decimal](19, 5) NOT NULL,
    [col_float_6] [decimal](19, 5) NOT NULL,
    [col_float_7] [decimal](19, 5) NOT NULL,
    [col_float_8] [decimal](19, 5) NOT NULL,
    [col_float_9] [decimal](19, 5) NOT NULL,
    [col_float_10] [decimal](19, 5) NOT NULL,
    [col_float_11] [decimal](19, 5) NOT NULL,
    [col_float_12] [decimal](19, 5) NOT NULL,
    [col_float_13] [decimal](19, 5) NOT NULL,
    [col_float_14] [decimal](19, 5) NOT NULL,
    [col_float_15] [decimal](19, 5) NOT NULL,
    [col_float_16] [decimal](19, 5) NOT NULL,
    [col_float_17] [decimal](19, 5) NOT NULL,
    [col_float_18] [decimal](19, 5) NOT NULL,
    [col_float_19] [decimal](19, 5) NOT NULL,
    [col_float_20] [decimal](19, 5) NOT NULL,
    [col_float_21] [decimal](19, 5) NULL,
    [col_float_22] [decimal](19, 5) NULL,
    [col_float_23] [decimal](19, 5) NULL,
    [col_float_24] [decimal](19, 5) NULL
) on ps_batch_id (batch_id);

-- This table will only hold data temporarily and can be dropped/recreated at any time
drop table if exists user_data_cleanup;
create table user_data_cleanup (
    [batch_id] [int] NOT NULL,
    [col_int_1] [int] NOT NULL,
    [col_int_2] [int] NOT NULL,
    [col_int_3] [int] NOT NULL,
    [col_float_1] [decimal](19, 5) NOT NULL,
    [col_float_2] [decimal](19, 5) NOT NULL,
    [col_float_3] [decimal](19, 5) NOT NULL,
    [col_float_4] [decimal](19, 5) NOT NULL,
    [col_float_5] [decimal](19, 5) NOT NULL,
    [col_float_6] [decimal](19, 5) NOT NULL,
    [col_float_7] [decimal](19, 5) NOT NULL,
    [col_float_8] [decimal](19, 5) NOT NULL,
    [col_float_9] [decimal](19, 5) NOT NULL,
    [col_float_10] [decimal](19, 5) NOT NULL,
    [col_float_11] [decimal](19, 5) NOT NULL,
    [col_float_12] [decimal](19, 5) NOT NULL,
    [col_float_13] [decimal](19, 5) NOT NULL,
    [col_float_14] [decimal](19, 5) NOT NULL,
    [col_float_15] [decimal](19, 5) NOT NULL,
    [col_float_16] [decimal](19, 5) NOT NULL,
    [col_float_17] [decimal](19, 5) NOT NULL,
    [col_float_18] [decimal](19, 5) NOT NULL,
    [col_float_19] [decimal](19, 5) NOT NULL,
    [col_float_20] [decimal](19, 5) NOT NULL,
    [col_float_21] [decimal](19, 5) NULL,
    [col_float_22] [decimal](19, 5) NULL,
    [col_float_23] [decimal](19, 5) NULL,
    [col_float_24] [decimal](19, 5) NULL
) on ps_batch_id (batch_id);

