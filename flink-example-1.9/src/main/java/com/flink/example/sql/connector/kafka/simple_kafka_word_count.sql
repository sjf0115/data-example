CREATE TABLE kafka_word_source_table (
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

CREATE TABLE kafka_word_sink_table (
  word STRING COMMENT '单词',
  frequency bigint COMMENT '次数'
) WITH (
  'connector.type' = 'kafka',
  'connector.version' = 'universal',
  'connector.topic' = 'word',
  'connector.properties.bootstrap.servers' = 'localhost:9092',
  'connector.properties.group.id' = 'kafka-connector-word',
  'connector.startup-mode' = 'earliest-offset',
  'format.type' = 'json',
  'update-mode' = 'append'
);