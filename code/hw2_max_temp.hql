-- Homework Assignment 2 - Q4: Hive Table Creation, Data Load, and Max Temperature Query (Pages 416-417)

SET hive.metastore.warehouse.dir;

CREATE TABLE IF NOT EXISTS records_tbl (
  year STRING,
  temperature INT,
  quality INT
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/home/student13/hive/records_tbl';

LOAD DATA LOCAL INPATH '/home/student13/sample_1.txt'
  OVERWRITE INTO TABLE records_tbl;

SELECT year, MAX(temperature)
FROM records_tbl
WHERE temperature != 9999
  AND (quality = 0 OR quality = 1 OR quality = 4 OR quality = 5 OR quality = 9)
GROUP BY year;
