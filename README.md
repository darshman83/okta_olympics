# okta_olympics
Demo snowflake pipeline for streaming json data

1. Instantiate_S3_Pipe.sql is executed to build pipe from source S3 bucket. Any json files that are dropped here will be ingested into raw landing table in snowflake
2. Create_StagingTable_Task.sql is executed to schedule a task daily at 5pm EST time to pull rows from raw landing table and load into staging table to source all downstream reports
3. Create_ReportingTable_Task.sql is executed to create task to summarize the staging table once dependency is completed and create reporting table in snowflake to be referenced by tableau
