CREATE TABLE kafka_word_count_table (
  word STRING COMMENT '单词',
  frequency BIGINT COMMENT '次数'
) WITH (
  'connector' = 'kafka',
  'topic' = 'word',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'kafka-connector-word',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true',
  'json.fail-on-missing-field' = 'false'
)