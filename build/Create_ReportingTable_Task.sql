--This code will create the reporting table for tableau report
--The task will be executed once staging table task completes
CREATE OR REPLACE TASK create_olympics_reporting_table 
WAREHOUSE = compute_wh
AFTER CREATE_OLYMPICS_STAGED
AS

CREATE OR REPLACE TABLE olympics_dev.olympics_analytics.olympics_reporting AS
with cleansed_countries as (
    SELECT *,
    REGEXP_REPLACE(team, '-[0-9]+$', '') as unique_team
    FROM staging.olympics.olympics_data_staging
)

SELECT year, season, COUNT(distinct unique_team) as countries_w_medals
FROM cleansed_countries
GROUP BY year, season;

--TESTING CODE
-- SELECT * FROM olympics_data_staging LIMIT 10;

-- SELECT DISTINCT team FROM olympics_data_staging;

-- SELECT DISTINCT REGEXP_REPLACE(team, '-[0-9]+$', '') FROM olympics_data_staging;
