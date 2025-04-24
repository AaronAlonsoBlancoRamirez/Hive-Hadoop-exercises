-- 0) Ajustes MR para aprovechar memoria y compresión
SET mapreduce.map.memory.mb=2048;
SET mapreduce.map.java.opts=-Xmx1638m;
SET mapreduce.task.io.sort.mb=512;
SET mapreduce.map.sort.spill.percent=0.80;
SET mapreduce.map.output.compress=true;
SET mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET yarn.app.mapreduce.am.resource.mb=1024;
SET yarn.app.mapreduce.am.command-opts=-Xmx768m;

-- 1) Definimos tabla externa sobre /visits
CREATE EXTERNAL TABLE IF NOT EXISTS visits (
  user_id STRING,
  url     STRING,
  ts      STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/visits';

-- 2) Calculamos el promedio de visitas por usuario
INSERT OVERWRITE DIRECTORY '/hive_avg_visits'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT
  AVG(num_visits) AS avg_visits
FROM (
  SELECT user_id
       , COUNT(*)   AS num_visits
  FROM visits
  GROUP BY user_id
) t;