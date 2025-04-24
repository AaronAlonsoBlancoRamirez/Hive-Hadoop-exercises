-- 0) ajustes MR (optimización)
SET mapreduce.map.memory.mb=2048;
SET mapreduce.map.java.opts=-Xmx1638m;
SET mapreduce.task.io.sort.mb=512;
SET mapreduce.map.sort.spill.percent=0.80;
SET mapreduce.map.output.compress=true;
SET mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET yarn.app.mapreduce.am.resource.mb=1024;
SET yarn.app.mapreduce.am.command-opts=-Xmx768m;

-- 1) tabla externa sobre /userlogs
CREATE EXTERNAL TABLE IF NOT EXISTS user_logs (
  user_id   STRING,
  ts        BIGINT,
  action    STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/userlogs';

-- 2) cuenta entradas por usuario
INSERT OVERWRITE DIRECTORY '/hive_userlog_counts'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT
  user_id,
  COUNT(*) AS cnt
FROM user_logs
GROUP BY user_id
ORDER BY user_id;
