-- 0) ajustes MR
SET mapreduce.map.memory.mb=2048;
SET mapreduce.map.java.opts=-Xmx1638m;
SET mapreduce.task.io.sort.mb=512;
SET mapreduce.map.sort.spill.percent=0.80;
SET mapreduce.map.output.compress=true;
SET mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET yarn.app.mapreduce.am.resource.mb=1024;
SET yarn.app.mapreduce.am.command-opts=-Xmx768m;

-- 1) tabla externa
CREATE EXTERNAL TABLE IF NOT EXISTS raw_text (
  line STRING
)
STORED AS TEXTFILE
LOCATION '/input';

-- 2) wordcount
INSERT OVERWRITE DIRECTORY '/hive_wc_output'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT
  word,
  COUNT(1) AS cnt
FROM raw_text
LATERAL VIEW explode(split(line, '\\s+')) t AS word
GROUP BY word;
