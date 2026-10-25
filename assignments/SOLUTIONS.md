# Complete Solutions: Course Project, Homework 1, Homework 2 & Lab Exercises

This document contains the complete, verified code, cluster execution commands, and outputs for **Homework Assignment 1**, **Homework Assignment 2**, and the **3-Part Course Project** in **BAN 632: Big Data Technology and Applications**.

---

## 1. Course Project (3-Part End-to-End NCDC Weather Pipeline)

**Problem Statement ([`CourseProject-1.docx`](CourseProject-1.docx)):**
1. **Part 1 (MRJob Extraction)**: Develop a Python MRJob application to retrieve `Year` and `Temperature` from the original NCDC records (`data/NewProjectData/*.gz`) and write the `Year` and `Temperature` data into a text file ([`GKoutput.txt`](GKoutput.txt)).
2. **Part 2 (Apache Pig)**: Load the text file into Pig and compute the **highest** and **lowest** temperatures for each year.
3. **Part 3 (Apache Hive)**: Load the text file into Hive and compute the **average** temperature for each year.

### Part 1: Python MRJob Extraction ([`code/TempExtractor_Python2.py`](../code/TempExtractor_Python2.py))

#### Hadoop Execution Commands
```bash
# 1. Create HDFS directory and upload the 100 NCDC .gz station files (1930-1939)
hdfs dfs -mkdir -p /home/13student13/project/data
hdfs dfs -put /home/student13/ProjectData/* /home/13student13/project/data/
hdfs dfs -ls /home/13student13/project/data

# 2. Run MRJob on Hadoop YARN
python TempExtractor_Python2.py -r hadoop \
  hdfs:///home/13student13/project/data/* \
  -o hdfs:///home/13student13/project/prj_output/

# 3. Merge all reducer part files into GKoutput.txt (130,421 records)
hdfs dfs -ls /home/13student13/project/prj_output/
hdfs dfs -cat /home/13student13/project/prj_output/part-* > GKoutput.txt
head -n 10 GKoutput.txt
```

#### Sample Output ([`assignments/GKoutput.txt`](GKoutput.txt) — 130,421 total rows)
```text
1934	72
1934	72
1934	72
1934	67
1934	78
1934	83
1934	78
1934	78
1934	78
1934	67
```

---

### Part 2: Apache Pig — Highest & Lowest Temperature per Year ([`code/project_min_max_temp.pig`](../code/project_min_max_temp.pig))

#### HDFS & Grunt Commands
```bash
hdfs dfs -copyFromLocal /home/student13/GKoutput.txt /home/13student13/project
hdfs dfs -ls /home/13student13/project
pig
```
```pig
all_records = LOAD '/home/13student13/project/GKoutput.txt'
  AS (year:chararray, temperature:int);

grouped_records = GROUP all_records BY year;

max_min_temp = FOREACH grouped_records GENERATE
  group AS year,
  MAX(all_records.temperature) AS max_temp,
  MIN(all_records.temperature) AS min_temp;

Sorted_byYear = ORDER max_min_temp BY year;
DUMP Sorted_byYear;
```

#### Final Pig Output (Tenths of °C)
```text
(1930,233,-139)
(1931,328,-250)
(1932,256,-161)
(1933,272,-261)
(1934,300,-172)
(1935,256,-189)
(1936,272,-189)
(1937,472,-178)
(1938,472,-200)
(1939,272,-261)
```

---

### Part 3: Apache Hive — Average Temperature per Year ([`code/project_avg_temp.hql`](../code/project_avg_temp.hql))

#### HiveQL Commands
```bash
hdfs dfs -mkdir -p /home/13student13/project/output_file
hdfs dfs -copyFromLocal /home/student13/GKoutput.txt /home/13student13/project/output_file/
hive
```
```sql
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
```

#### Final Hive Output (Tenths of °C)
```text
1930	79.35
1931	71.37
1932	77.55
1933	80.72
1934	83.41
1935	75.72
1936	78.48
1937	78.77
1938	85.87
1939	80.54
```

---

## 2. Homework Assignment 1 ([`HomeworkAssignment1.docx`](HomeworkAssignment1.docx))

### Q1: Rewrite Java Examples 2-3, 2-4, and 2-5 in Python (Hadoop Streaming)
- **Mapper**: [`code/max_temperature_map.py`](../code/max_temperature_map.py)
- **Reducer**: [`code/max_temperature_reduce.py`](../code/max_temperature_reduce.py)

#### Commands
```bash
# Local pipeline test
cat data/sample.txt | python code/max_temperature_map.py | sort | python code/max_temperature_reduce.py

# Cluster execution via Hadoop Streaming
hdfs dfs -copyFromLocal /home/student13/sample.txt /home/13student13/sample.txt
hadoop jar /home/student13/hadoop-streaming-2.9.0.jar \
  -input /home/13student13/sample.txt \
  -output /home/13student13/output12 \
  -file /home/student13/max_temperature_map.py \
  -mapper /home/student13/max_temperature_map.py \
  -file /home/student13/max_temperature_reduce.py \
  -reducer /home/student13/max_temperature_reduce.py

hdfs dfs -ls /home/13student13/output12/
hdfs dfs -cat /home/13student13/output12/part-00000
```

#### Output
```text
1949	111
1950	22
```

---

### Q2: Work Example 3-3 (`FileSystemDoubleCat`) on Hadoop
- **Source File**: [`code/FileSystemDoubleCat.java`](../code/FileSystemDoubleCat.java) (and [`code/URLCat.java`](../code/URLCat.java) for Example 3-1)

#### Commands
```bash
hdfs dfs -copyFromLocal /home/student13/quangle.txt /home/13student13/quangle.txt

javac -classpath /home/student13/hadoop-common-2.6.1.jar:/home/student13/hadoop-mapreduce-client-core-2.6.1.jar:/home/student13/commons-cli-2.0.jar \
  -d . FileSystemDoubleCat.java

jar -cvf FileSystemDoubleCat.jar FileSystemDoubleCat.class

hadoop jar FileSystemDoubleCat.jar FileSystemDoubleCat /home/13student13/quangle.txt
```

#### Output
```text
On the top of the Crumpetty Tree
The Quangle Wangle sat,
But his face you could not see,
On account of his Beaver Hat.
On the top of the Crumpetty Tree
The Quangle Wangle sat,
But his face you could not see,
On account of his Beaver Hat.
```

---

### Q3: Work Example 4-16 (`MapFileWriteDemo`) on Hadoop
- **Source File**: [`code/MapFileWriteDemo.java`](../code/MapFileWriteDemo.java)

#### Commands
```bash
javac -classpath /home/student13/hadoop-common-2.6.1.jar:/home/student13/hadoop-mapreduce-client-core-2.6.1.jar:/home/student13/commons-cli-2.0.jar:/home/student13/commons-logging-1.2.jar \
  -d . /home/student13/MapFileWriteDemo.java

jar -cvf map-filewritedemo.jar MapFileWriteDemo.class

hdfs dfs -mkdir -p /home/13student13/output-GKAS-1
hadoop jar map-filewritedemo.jar MapFileWriteDemo /home/13student13/output-GKAS-1/numbers.map

hdfs dfs -ls /home/13student13/output-GKAS-1/numbers.map
hdfs dfs -text /home/13student13/output-GKAS-1/numbers.map/data | head
hdfs dfs -text /home/13student13/output-GKAS-1/numbers.map/index
```

#### Output (`data | head` and `index`)
```text
# data (first 10 records):
1	One, two, buckle my shoe
2	Three, four, shut the door
3	Five, six, pick up sticks
4	Seven, eight, lay them straight
5	Nine, ten, a big fat hen
6	One, two, buckle my shoe
7	Three, four, shut the door
8	Five, six, pick up sticks
9	Seven, eight, lay them straight
10	Nine, ten, a big fat hen

# index (every 128th key):
1	128
129	6079
257	12054
385	18030
513	24002
641	29976
769	35949
897	41922
```

---

### Q4: Spark `WordCountSP.py` on `quangle.txt`
- **Source File**: [`code/WordCountSP.py`](../code/WordCountSP.py) (and MRJob version [`code/word_count.py`](../code/word_count.py))

#### Commands
```bash
hdfs dfs -copyFromLocal /home/student13/quangle.txt /home/13student13/quangle.txt
spark-submit --master local /home/student13/WordCountSP.py
hdfs dfs -ls /home/13student13/output20
hdfs dfs -cat /home/13student13/output20/part-*
```

#### Output
```text
('On', 2)
('the', 2)
('top', 1)
('of', 2)
('Crumpetty', 1)
('Tree', 1)
('The', 1)
('Quangle', 1)
('Wangle', 1)
('sat,', 1)
('But', 1)
('his', 2)
('face', 1)
('you', 1)
('could', 1)
('not', 1)
('see,', 1)
('account', 1)
('Beaver', 1)
('Hat.', 1)
```

---

## 3. Homework Assignment 2 ([`HomeworkAssignment2-1.docx`](HomeworkAssignment2-1.docx))

### Q1: Python MRJob — Retrieve and Count `-10°C` Temperatures in `1930`
- **Source File**: [`code/neg10_1930_PYTHON2.py`](../code/neg10_1930_PYTHON2.py)

#### Commands
```bash
hdfs dfs -mkdir -p /home/13student13/ncdc/1930
hdfs dfs -put /home/student13/1930/* /home/13student13/ncdc/1930/
hdfs dfs -ls /home/13student13/ncdc/1930

python neg10_1930_PYTHON2.py -r hadoop \
  hdfs:///home/13student13/ncdc/1930/* \
  -o hdfs:///home/13student13/output_1930/

hdfs dfs -ls /home/13student13/output_1930/
hdfs dfs -cat /home/13student13/output_1930/part-00000
```

#### Output (24 Matching Records + Total Count)
```text
["COUNT", "TOTAL"]	24
["MATCH", "000012-75099\t19301231\t-10.0C"]	1
["MATCH", "000033-93099\t19301229\t-10.0C"]	1
["MATCH", "004302-61099\t19301223\t-10.0C"]	1
["MATCH", "004310-53099\t19301226\t-10.0C"]	1
["MATCH", "005010-35099\t19300211\t-10.0C"]	1
["MATCH", "005026-02099\t19300209\t-10.0C"]	1
["MATCH", "005133-93099\t19300208\t-10.0C"]	1
["MATCH", "005402-61099\t19300206\t-10.0C"]	1
["MATCH", "005433-19099\t19300209\t-10.0C"]	1
["MATCH", "005633-19099\t19300125\t-10.0C"]	1
["MATCH", "005633-93099\t19300121\t-10.0C"]	1
["MATCH", "005710-53099\t19301227\t-10.0C"]	1
["MATCH", "005726-10099\t19300201\t-10.0C"]	1
["MATCH", "005733-93099\t19300210\t-10.0C"]	1
["MATCH", "006402-61099\t19300318\t-10.0C"]	1
["MATCH", "006406-80099\t19300213\t-10.0C"]	1
["MATCH", "007010-65099\t19300222\t-10.0C"]	1
["MATCH", "007010-65099\t19300224\t-10.0C"]	1
["MATCH", "007010-66099\t19300222\t-10.0C"]	1
["MATCH", "007010-66099\t19300224\t-10.0C"]	1
["MATCH", "007026-02099\t19301228\t-10.0C"]	1
["MATCH", "007733-93099\t19300120\t-10.0C"]	1
["MATCH", "008126-02099\t19301217\t-10.0C"]	1
["MATCH", "010226-47099\t19300209\t-10.0C"]	1
```

---

### Q2: Five Pig Examples on Pages 384–385 ("Schemas" to "Validation and nulls")
- **Source File**: [`code/hw2_schemas_validation.pig`](../code/hw2_schemas_validation.pig)

#### Outputs
1. **With schema and types (`AS (year:int, temperature:int, quality:int)`)**:
   - `DESCRIBE records;` $\rightarrow$ `records: {year: int,temperature: int,quality: int}`
   - `DUMP records;` $\rightarrow$ `(1950,0,1)`, `(1950,22,1)`, `(1950,-11,1)`, `(1949,111,1)`, `(1949,78,1)`
2. **With field names only (`AS (year, temperature, quality)`)**:
   - `DESCRIBE records;` $\rightarrow$ `records: {year: bytearray,temperature: bytearray,quality: bytearray}`
3. **Without schema**:
   - `DESCRIBE records;` $\rightarrow$ `Schema for records unknown.`
4. **Loading `sample_corrupt.txt` (`e9999` temperature value)**:
   - `DUMP corrupt_records;` $\rightarrow$ `(1950,0,1)`, `(1950,22,1)`, `(1950,,1)`, `(1949,111,1)`, `(1949,78,1)`
   - `FILTER corrupt_records BY temperature IS NULL;` $\rightarrow$ `(1950,,1)`
   - `COUNT(corrupted_records)` $\rightarrow$ `(all,1)`
5. **Splitting into `good_records` and `bad_records`**:
   - `DUMP good_records;` $\rightarrow$ `(1950,0,1)`, `(1950,22,1)`, `(1949,111,1)`, `(1949,78,1)`
   - `DUMP bad_records;` $\rightarrow$ `(1950,,1)`

---

### Q3: Pig Custom Load UDF (`CutLoadFunc` & `Range` — Pages 396–397)
- **Source Files**: [`code/CutLoadFunc.java`](../code/CutLoadFunc.java) and [`code/Range.java`](../code/Range.java)

#### Compilation & Execution Commands
```bash
javac -classpath /home/student13/hadoop-common-2.6.1.jar:/home/student13/hadoop-mapreduce-client-core-2.6.1.jar:/home/student13/commons-cli-2.0.jar:/home/student13/commons-logging-1.2.jar:/home/student13/pig-0.11.0.jar \
  -d /home/student13 \
  /home/student13/Range.java /home/student13/CutLoadFunc.java

jar -cvf /home/student13/CutLoadFunc.jar \
  -C /home/student13 com/hadoopbook/pig/Range.class \
  -C /home/student13 com/hadoopbook/pig/CutLoadFunc.class
```
```pig
REGISTER /home/student13/CutLoadFunc.jar;
record = LOAD '/home/13student13/input/sample.txt'
  USING com.hadoopbook.pig.CutLoadFunc('16-19,88-92,93-93')
  AS (year:int, temperature:int, quality:int);
DUMP record;
```

#### Output
```text
(1950,0,1)
(1950,22,1)
(1950,-11,1)
(1949,111,1)
(1949,78,1)
```

---

### Q4: Four Hive Examples on Pages 416–417 (`records_tbl` & Max Temperature)
- **Source File**: [`code/hw2_max_temp.hql`](../code/hw2_max_temp.hql)

#### Commands
```bash
# Re-initialize Derby metastore if needed
ls -l | grep meta
mv metastore_db metastore_db.old
schematool -dbType derby -initSchema
hive
```
```sql
SET hive.metastore.warehouse.dir;

CREATE TABLE records_tbl (
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
```

#### Output
```text
1949	111
1950	22
```
