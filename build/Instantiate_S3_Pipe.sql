--This script will stage .json files in target s3 directory and load into
-- landing table in the raw database
-- NOTE: You will need to configure the SQS queue in s3

-------------------------------------------------------------------------
-- CREATE STAGE FOR RAW OLYMPIC JSON DATA
-------------------------------------------------------------------------
CREATE OR REPLACE STAGE my_stage
  URL = 's3://olympic-raw/'
  CREDENTIALS = (AWS_KEY_ID = 'xxxxxxxxxxxxxxxxxxxxxxxxxxx'
                 AWS_SECRET_KEY = 'xxxxxxxxxxxxxxxxxxxxxxxx')
  FILE_FORMAT = (TYPE = 'JSON');
-------------------------------------------------------------------------
-- CREATE FILE FORMAT FOR RAW OLYMPIC JSON DATA
-------------------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT json_file_format
  TYPE = 'JSON'
  COMPRESSION = 'AUTO'
  ENABLE_OCTAL = FALSE
  STRIP_OUTER_ARRAY = TRUE;
-------------------------------------------------------------------------
-- CREATE TARGET TABLE FOR RAW OLYMPIC JSON DATA
-------------------------------------------------------------------------
CREATE OR REPLACE TABLE olympic_raw_ingest (
 json_text variant,
 file_name text,
 load_timestamp datetime
);
-------------------------------------------------------------------------
-- CREATE PIPE TO AUTO MONITOR EXTERNAL S3 STAGE
-------------------------------------------------------------------------
CREATE OR REPLACE PIPE raw_olympics_pipe
  AUTO_INGEST = TRUE
  AS
  COPY INTO olympic_raw_ingest
  FROM (SELECT $1, METADATA$FILENAME, current_timestamp() as load_timestamp
  FROM @my_stage)
  FILE_FORMAT = json_file_format;