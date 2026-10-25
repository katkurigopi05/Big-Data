-- Course Project Part 3: HiveQL Script to Calculate Yearly Average Temperature
-- Input: GKoutput.txt (<year>\t<temperature> in tenths of degrees Celsius)

CREATE DATABASE IF NOT EXISTS GKweather;
USE GKweather;

DROP TABLE IF EXISTS GK_year_temp;

CREATE EXTERNAL TABLE GK_year_temp (
  year STRING,
  temp INT
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION 'hdfs:///home/13student13/project/output_file/';

CREATE VIEW IF NOT EXISTS GKyear_avg_temp AS
SELECT
  year,
  ROUND(AVG(temp), 2) AS avg_temp_tenthsC
FROM GK_year_temp
GROUP BY year
ORDER BY year;

SELECT * FROM GKyear_avg_temp;
