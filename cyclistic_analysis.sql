
/*
01_create_trips_all.sql

Purpose:
Combine all monthly Cyclistic trip tables into one master dataset.

Key Transformation:
- Cast start_station_id and end_station_id to STRING to resolve data type inconsistencies across months.

Output:
cyclistic_analysis.trips_all
*/


CREATE OR REPLACE TABLE `gen-lang-client-0849675694.cyclistic_analysis.trips_all` AS

SELECT
  ride_id, rideable_type, started_at, ended_at,
  start_station_name,
  CAST(start_station_id AS STRING) AS start_station_id,
  end_station_name,
  CAST(end_station_id AS STRING) AS end_station_id,
  start_lat, start_lng, end_lat, end_lng, member_casual
FROM `gen-lang-client-0849675694.cyclistic_analysis.trips_202004`

UNION ALL

SELECT
  ride_id, rideable_type, started_at, ended_at,
  start_station_name,
  CAST(start_station_id AS STRING) AS start_station_id,
  end_station_name,
  CAST(end_station_id AS STRING) AS end_station_id,
  start_lat, start_lng, end_lat, end_lng, member_casual
FROM `gen-lang-client-0849675694.cyclistic_analysis.trips_202005`

UNION ALL

SELECT
  ride_id, rideable_type, started_at, ended_at,
  start_station_name,
  CAST(start_station_id AS STRING) AS start_station_id,
  end_station_name,
  CAST(end_station_id AS STRING) AS end_station_id,
  start_lat, start_lng, end_lat, end_lng, member_casual
FROM `gen-lang-client-0849675694.cyclistic_analysis.trips_202006`

UNION ALL

SELECT
  ride_id, rideable_type, started_at, ended_at,
  start_station_name,
  CAST(start_station_id AS STRING) AS start_station_id,
  end_station_name,
  CAST(end_station_id AS STRING) AS end_station_id,
  start_lat, start_lng, end_lat, end_lng, member_casual
FROM `gen-lang-client-0849675694.cyclistic_analysis.trips_202007`

UNION ALL

SELECT
  ride_id, rideable_type, started_at, ended_at,
  start_station_name,
  CAST(start_station_id AS STRING) AS start_station_id,
  end_station_name,
  CAST(end_station_id AS STRING) AS end_station_id,
  start_lat, start_lng, end_lat, end_lng, member_casual
FROM `gen-lang-client-0849675694.cyclistic_analysis.trips_202009`

UNION ALL

SELECT
  ride_id, rideable_type, started_at, ended_at,
  start_station_name,
  CAST(start_station_id AS STRING) AS start_station_id,
  end_station_name,
  CAST(end_station_id AS STRING) AS end_station_id,
  start_lat, start_lng, end_lat, end_lng, member_casual
FROM `gen-lang-client-0849675694.cyclistic_analysis.trips_202010`

UNION ALL

SELECT
  ride_id, rideable_type, started_at, ended_at,
  start_station_name,
  CAST(start_station_id AS STRING) AS start_station_id,
  end_station_name,
  CAST(end_station_id AS STRING) AS end_station_id,
  start_lat, start_lng, end_lat, end_lng, member_casual
FROM `gen-lang-client-0849675694.cyclistic_analysis.trips_202011`

UNION ALL

SELECT
  ride_id, rideable_type, started_at, ended_at,
  start_station_name,
  CAST(start_station_id AS STRING) AS start_station_id,
  end_station_name,
  CAST(end_station_id AS STRING) AS end_station_id,
  start_lat, start_lng, end_lat, end_lng, member_casual
FROM `gen-lang-client-0849675694.cyclistic_analysis.trips_202012`

UNION ALL

SELECT
  ride_id, rideable_type, started_at, ended_at,
  start_station_name,
  CAST(start_station_id AS STRING) AS start_station_id,
  end_station_name,
  CAST(end_station_id AS STRING) AS end_station_id,
  start_lat, start_lng, end_lat, end_lng, member_casual
FROM `gen-lang-client-0849675694.cyclistic_analysis.trips_202101`

UNION ALL

SELECT
  ride_id, rideable_type, started_at, ended_at,
  start_station_name,
  CAST(start_station_id AS STRING) AS start_station_id,
  end_station_name,
  CAST(end_station_id AS STRING) AS end_station_id,
  start_lat, start_lng, end_lat, end_lng, member_casual
FROM `gen-lang-client-0849675694.cyclistic_analysis.trips_202102`

UNION ALL

SELECT
  ride_id, rideable_type, started_at, ended_at,
  start_station_name,
  CAST(start_station_id AS STRING) AS start_station_id,
  end_station_name,
  CAST(end_station_id AS STRING) AS end_station_id,
  start_lat, start_lng, end_lat, end_lng, member_casual
FROM `gen-lang-client-0849675694.cyclistic_analysis.trips_202103`


;

/*
02_create_trips_clean.sql

Purpose:
Clean the combined trips dataset and create ride duration.

Key Transformations:
- Calculate ride_length_minutes using TIMESTAMP_DIFF
- Remove rides with duration <= 0
- Remove rides longer than 24 hours (outliers)

Output:
cyclistic_analysis.trips_clean
*/

CREATE OR REPLACE TABLE `gen-lang-client-0849675694.cyclistic_analysis.trips_clean` AS
SELECT *
FROM (
  SELECT
    *,
    TIMESTAMP_DIFF(ended_at, started_at, MINUTE) AS ride_length_minutes
  FROM `gen-lang-client-0849675694.cyclistic_analysis.trips_all`
)
WHERE ride_length_minutes > 0
  AND ride_length_minutes < 1440;


/*
03_analysis_queries.sql

Purpose:
Analyze rider behavior differences between members and casual riders.
*/

-- Query 1: Average ride length
SELECT
  member_casual,
  ROUND(AVG(ride_length_minutes), 2) AS avg_ride_length
FROM `gen-lang-client-0849675694.cyclistic_analysis.trips_clean`
GROUP BY member_casual;

-- Query 2: Rides by day of week
SELECT
  member_casual,
  FORMAT_TIMESTAMP('%A', started_at) AS day_name,
  COUNT(*) AS total_rides
FROM `gen-lang-client-0849675694.cyclistic_analysis.trips_clean`
GROUP BY member_casual, day_name;

-- Query 3: Time of day
SELECT
  member_casual,
  CASE
    WHEN EXTRACT(HOUR FROM started_at) BETWEEN 6 AND 9 THEN 'Morning'
    WHEN EXTRACT(HOUR FROM started_at) BETWEEN 10 AND 15 THEN 'Midday'
    WHEN EXTRACT(HOUR FROM started_at) BETWEEN 16 AND 18 THEN 'Evening'
    ELSE 'Night'
  END AS time_block,
  COUNT(*) AS total_rides
FROM `gen-lang-client-0849675694.cyclistic_analysis.trips_clean`
GROUP BY member_casual, time_block;





