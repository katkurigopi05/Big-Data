-- Course Project Part 2: Pig Script to Calculate Yearly Highest and Lowest Temperatures
-- Input: GKoutput.txt (<year>\t<temperature> in tenths of degrees Celsius)

all_records = LOAD '/home/13student13/project/GKoutput.txt'
  AS (year:chararray, temperature:int);

grouped_records = GROUP all_records BY year;

max_min_temp = FOREACH grouped_records GENERATE
  group AS year,
  MAX(all_records.temperature) AS max_temp,
  MIN(all_records.temperature) AS min_temp;

Sorted_byYear = ORDER max_min_temp BY year;

DUMP Sorted_byYear;
