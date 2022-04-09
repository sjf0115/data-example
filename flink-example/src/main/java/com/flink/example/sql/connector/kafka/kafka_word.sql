CREATE TABLE kafka_word_table (
  word STRING COMMENT '单词',
  frequency bigint COMMENT '次数'
) WITH (
  'connector' = 'kafka',
  'topic' = 'word',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'kafka-connector-word',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true',
  'json.fail-on-missing-field' = 'true'
);

CREATE TABLE kafka_word_table (
  word STRING COMMENT '单词',
  frequency bigint COMMENT '次数'
) WITH (
  'connector.type' = 'kafka',
  'connector.version' = 'universal',
  'connector.topic' = 'word',
  'connector.properties.bootstrap.servers' = 'localhost:9092',
  'connector.properties.group.id' = 'kafka-connector-word',
  'connector.startup-mode' = 'earliest-offset',
  'format.type' = 'json'
);