CREATE TABLE word_count (
  word BIGINT COMMENT '单词',
  count BIGINT COMMENT '出现次数'
) WITH (
  'connector' = 'print'
)

INSERT INTO word_count
SELECT
  word, COUNT(*) AS cnt
FROM words
GROUP BY word