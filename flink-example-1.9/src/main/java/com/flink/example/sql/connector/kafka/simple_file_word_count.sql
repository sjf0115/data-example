CREATE TABLE file_source_table (
  word STRING COMMENT '单词',
  frequency bigint COMMENT '次数'
) WITH (
  'connector.type' = 'filesystem',
  'connector.path' = '/opt/data/word_count_input.csv',
  'format.type' = 'csv',
  'format.fields.0.name' = 'word',
  'format.fields.0.data-type' = 'STRING',
  'format.fields.1.name' = 'frequency',
  'format.fields.1.data-type' = 'bigint'
)

CREATE TABLE file_sink_table (
  word STRING COMMENT '单词',
  frequency bigint COMMENT '次数'
) WITH (
  'connector.type' = 'filesystem',
  'connector.path' = '/opt/data/word_count_output.csv',
  'format.type' = 'csv',
  'format.fields.0.name' = 'word',
  'format.fields.0.data-type' = 'STRING',
  'format.fields.1.name' = 'frequency',
  'format.fields.1.data-type' = 'bigint'
)


INSERT INTO file_sink_table
SELECT word, SUM(frequency) AS frequency
FROM file_source_table
GROUP BY word