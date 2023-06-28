--This code creates a task that will create a staging table for olympic data
--The code will execute once daily at 5pm EST time (can be configured in CRON config)

CREATE OR REPLACE TASK create_olympics_staged
  WAREHOUSE = compute_wh
  SCHEDULE = 'USING CRON 0 17 * * * America/New_York'
AS

--Create table that flattens out variant column of json file into table
--This will be the staging table used to build any reporting tables for BI downstream
CREATE OR REPLACE TABLE olympics_data_staging AS

 SELECT json_text:age::INT AS age,
  json_text:athlete_id::INT AS athlete_id,
  json_text:city::VARCHAR AS city,
  json_text:event::VARCHAR AS event,
  json_text:height::FLOAT AS height,
  json_text:name::VARCHAR AS name,
  json_text:noc::VARCHAR AS noc,
  json_text:season::VARCHAR AS season,
  json_text:sex::VARCHAR AS sex,
  json_text:sport::VARCHAR AS sport,
  json_text:team::VARCHAR AS team,
  json_text:weight::FLOAT AS weight,
  json_text:year::INT AS year,
  file_name,
  load_timestamp
  FROM raw.olympics.olympic_raw_ingest;


  ALTER TASK create_olympics_staged RESUME;