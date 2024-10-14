# Summary of Partition Methods and Results

## Introduction

This document summarizes the method and results of a proof-of-concept that uses partition switching to improve the performance of deleting a large number of rows from a SQL table. This table contains a batch_id column that identifies the rows to be deleted. This method is absed on partitioning the table on the same column (batch_id) and instead of deleting the rows, the entire partition is switched to an identical table that can be truncated entirely. 

![Partition Switch Workflow](partition_switch.png)

## Pre-Requisites

1. **Clone the main table's structure**: Create an identical table with the same schema as the original table.
1. **Create the stored procedures**: The stored procedures are here just to simplify the process, so it can easily be called from external applications (*such as Azure Data Factory*) with parameters for the batch_id to be processed.



## Workflow for data load

1. **Identify the batch_id**: Assign a new batch_id for the incoming rows, based on business logic.

2. **Create a new partition the batch_id**: Execute the usp_create_table_partition.

3. **Load data**.

## Workflow for data deletion

1. **Identify the batch_id**: Determine the batch_id for the rows that need to be deleted.

2. **Switch the partition**: Execute the usp_switch_partition, receives parameters for source and target table, as well as batch_id.

4. **Truncate the target table**: Minimally logged operation, faster than searching individual rows.

5. **Delete the partition**: Depends on use case, but should be executed to ensure the partition limit (*15000*) is not reached.

&nbsp;

&nbsp;

## Benchmarks:
- Azure SQL database, Hyperscale, 2 vCores
- Pre-loaded table with 74 million rows, no NULL values.
- Tables are indexed on `batch_id`

## Creating and loading into pre-existing partition
- DataFrame to SQL, run 1: 100.98 seconds
- DataFrame to SQL, run 2: 129.74 seconds
- DataFrame to SQL, run 3: 101.64 seconds
- DataFrame to SQL, run 4: 102.05 seconds
- DataFrame to SQL, run 5: 104.65 seconds
- DataFrame to SQL, run 6: 105.20 seconds

## Loading into no specific partition (table is still partitioned)
- DataFrame to SQL, run 1: 99.22 seconds
- DataFrame to SQL, run 2: 103.46 seconds
- DataFrame to SQL, run 3: 103.75 seconds
- DataFrame to SQL, run 4: 100.00 seconds
- DataFrame to SQL, run 5: 99.82 seconds
- DataFrame to SQL, run 6: 105.46 seconds

## Switching a partition
- **Command**: `EXECUTE dbo.usp_switch_partition 'user_data', 'user_data_cleanup', x`
- **Total time: 00:00:00.178**

## Deleting an empty partition
- **Command**: `EXECUTE dbo.usp_delete_table_partition 'user_data', x`
- Run 1: 00:00:00.901
- Run 2: 00:00:00.423
- Run 3: 00:00:00.622

## Deleting rows from partitioned table
- **Command**: `delete from user_data where batch_id = x;`
- Run 1: 00:00:20.796
- Run 2: 00:00:17.707
- Run 3: 00:00:16.571

## Deleting rows from NON-partitioned table
- **Command**: `delete from user_data_x where batch_id = x;`
- Run 1: 00:00:17.833
- Run 2: 00:00:17.233
- Run 3: 00:00:18.445

&nbsp;

&nbsp;

