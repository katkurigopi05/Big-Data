-- Homework Assignment 2 - Q2: Pig Schemas, Validation, and Nulls (Pages 384-385)

-- 1. Load with explicit schema and data types
records = LOAD '/home/13student13/input/sample-1.txt'
  AS (year:int, temperature:int, quality:int);
DESCRIBE records;
DUMP records;

-- 2. Load without data types (defaults to bytearray)
records_untyped = LOAD '/home/13student13/input/sample-1.txt'
  AS (year, temperature, quality);
DESCRIBE records_untyped;

-- 3. Load without schema
records_noschema = LOAD '/home/13student13/input/sample-1.txt';
DESCRIBE records_noschema;

-- 4. Validation and Nulls on corrupt input
corrupt_records = LOAD '/home/13student13/input/sample_corrupt.txt'
  AS (year:chararray, temperature:int, quality:int);
DUMP corrupt_records;

corrupted_records = FILTER corrupt_records BY temperature IS NULL;
DUMP corrupted_records;

group_corrupt = GROUP corrupted_records ALL;
all_grouped = FOREACH group_corrupt GENERATE group, COUNT(corrupted_records);
DUMP all_grouped;

-- 5. Splitting good and bad records
SPLIT corrupt_records INTO good_records IF temperature IS NOT NULL,
  bad_records IF temperature IS NULL;
DUMP good_records;
DUMP bad_records;
