-- 年
SELECT
    ts + INTERVAL '-1' YEAR        AS year_minus,  -- 1年    -- 2021-05-01 12:10:15.456
    ts + INTERVAL '+1' YEAR        AS year_add,    -- 1年    -- 2023-05-01 12:10:15.456
    ts + INTERVAL '10' YEAR(2)     AS year_p2,     -- 10年   -- 2032-05-01 12:10:15.456
    ts + INTERVAL '1000' YEAR(4)   AS year_p4      -- 1000年 -- 3022-05-01 12:10:15.456
FROM (
  -- 2022-05-01 12:10:15.456
  SELECT TO_TIMESTAMP_LTZ(1651378215456, 3) AS ts
);

-- 月
SELECT
    ts + INTERVAL '1-03' YEAR TO MONTH AS year_month,    -- 1年3个月 -- 2023-08-01 12:10:15.456
    ts + INTERVAL '15' MONTH           AS month_simple,  -- 15个月  -- 2023-08-01 12:10:15.456
    ts + INTERVAL '+3' MONTH           AS month_add,      -- 3个月   -- 2022-08-01 12:10:15.456
    ts + INTERVAL '-3' MONTH           AS month_minus     -- 3个月   -- 2022-02-01 12:10:15.456
FROM (
  -- 2022-05-01 12:10:15.456
  SELECT TO_TIMESTAMP_LTZ(1651378215456, 3) AS ts
);

-- 日
SELECT
    ts + INTERVAL '+1' DAY     AS day_add,    -- 1天    -- 2022-05-02 12:10:15.456
    ts + INTERVAL '-1' DAY     AS day_minus,  -- 1天    -- 2022-04-30 12:10:15.456
    ts + INTERVAL '10' DAY(2)  AS day_p2,     -- 10天   -- 2022-05-11 12:10:15.456
    ts + INTERVAL '365' DAY(3) AS day_p3      -- 365天  -- 2023-05-01 12:10:15.456
FROM (
  -- 2022-05-01 12:10:15.456
  SELECT TO_TIMESTAMP_LTZ(1651378215456, 3) AS ts
);

-- 小时
SELECT
    ts + INTERVAL '-1 03' DAY TO HOUR      AS day_hour_minus, -- 1天3小时  -- 2022-05-02 15:10:15.456
    ts + INTERVAL '1 03' DAY TO HOUR       AS day_hour,    -- 1天3小时  -- 2022-05-02 15:10:15.456
    ts + INTERVAL '10 03' DAY(2) TO HOUR   AS day_p2_hour, -- 10天3小时 -- 2022-05-11 15:10:15.456
    ts + INTERVAL '+3' HOUR                AS hour_add,    -- 3小时    -- 2022-05-01 15:10:15.456
    ts + INTERVAL '-3' HOUR                AS hour_minus   -- 3小时    -- 2022-05-01 09:10:15.456
FROM (
  -- 2022-05-01 12:10:15.456
  SELECT TO_TIMESTAMP_LTZ(1651378215456, 3) AS ts
);

-- 分钟
SELECT
    ts + INTERVAL '1 03:20' DAY TO MINUTE   AS day_minute,          -- 1天3小时20分钟 -- 2022-05-02 15:30:15.456
    ts + INTERVAL '-1 03:20' DAY TO MINUTE  AS day_minute_minus,    -- 1天3小时20分钟 -- 2022-04-30 08:50:15.456
    ts + INTERVAL '3:20' HOUR TO MINUTE     AS hour_to_minute,      -- 3小时20分钟    -- 2022-05-01 15:30:15.456
    ts + INTERVAL '-3:20' HOUR TO MINUTE    AS hour_to_minute_minus,-- 3小时20分钟    -- 2022-05-01 08:50:15.456
    ts + INTERVAL '+20' MINUTE              AS minute_add,          -- 20分钟        -- 2022-05-01 12:30:15.456
    ts + INTERVAL '-20' MINUTE              AS minute_minus         -- 20分钟        -- 2022-05-01 11:50:15.456
FROM (
  -- 2022-05-01 12:10:15.456
  SELECT TO_TIMESTAMP_LTZ(1651378215456, 3) AS ts
);

-- 秒
SELECT
    ts + INTERVAL '1 03:20:15' DAY TO SECOND        AS day_to_second,      -- 1天3小时20分钟15秒       -- 2022-05-02 15:30:30.456
    ts + INTERVAL '1 03:20:15.111' DAY TO SECOND(3) AS day_to_second_ms,   -- 1天3小时20分钟15秒111毫秒 -- 2022-05-02 15:30:30.567
    ts + INTERVAL '3:20:15' HOUR TO SECOND          AS hour_to_second,     -- 3小时20分钟15秒          -- 2022-05-01 15:30:30.456
    ts + INTERVAL '3:20:15.111' HOUR TO SECOND(3)   AS hour_to_second_ms,  -- 3小时20分钟15秒111毫秒    -- 2022-05-01 15:30:30.567
    ts + INTERVAL '20:15' MINUTE TO SECOND          AS minute_to_second,   -- 20分钟15秒              -- 2022-05-01 12:30:30.456
    ts + INTERVAL '20:15.111' MINUTE TO SECOND(3)   AS minute_to_second_ms,-- 20分钟15秒111毫秒        -- 2022-05-01 12:30:30.567
    ts + INTERVAL '15' SECOND                       AS second_add,         -- 15秒                   -- 2022-05-01 12:10:30.456
    ts + INTERVAL '-15' SECOND                      AS second_minus,       -- 15秒                   -- 2022-05-01 12:10:00.456
    ts + INTERVAL '15.111' SECOND(3)                AS second_ms_add       -- 15秒111毫秒             -- 2022-05-01 12:10:30.567
FROM (
  -- 2022-05-01 12:10:15.456
  SELECT TO_TIMESTAMP_LTZ(1651378215456, 3) AS ts
);


CREATE TABLE interval_sink_table (
  year_1   TIMESTAMP(3),
  year_2   TIMESTAMP(3),
  year_3   TIMESTAMP(3),
  year_4   TIMESTAMP(3),
  month_1  TIMESTAMP(3),
  month_2  TIMESTAMP(3),
  month_3  TIMESTAMP(3),
  month_4  TIMESTAMP(3),
  day_1    TIMESTAMP(3),
  day_2    TIMESTAMP(3),
  day_3    TIMESTAMP(3),
  day_4    TIMESTAMP(3),
  hour_1   TIMESTAMP(3),
  hour_2   TIMESTAMP(3),
  hour_3   TIMESTAMP(3),
  hour_4   TIMESTAMP(3),
  minute_1 TIMESTAMP(3),
  minute_2 TIMESTAMP(3),
  minute_3 TIMESTAMP(3),
  minute_4 TIMESTAMP(3),
  minute_5 TIMESTAMP(3),
  minute_6 TIMESTAMP(3),
  second_1 TIMESTAMP(3),
  second_2 TIMESTAMP(3),
  second_3 TIMESTAMP(3),
  second_4 TIMESTAMP(3),
  second_5 TIMESTAMP(3),
  second_6 TIMESTAMP(3),
  second_7 TIMESTAMP(3),
  second_8 TIMESTAMP(3),
  second_9 TIMESTAMP(3)
) WITH (
  'connector' = 'print',
  'print-identifier' = 'interval',
  'sink.parallelism' = '1'
)

INSERT INTO interval_sink_table
SELECT
    ts + INTERVAL '-1' YEAR                         AS year_1,    -- 1年
    ts + INTERVAL '+1' YEAR                         AS year_2,    -- 1年
    ts + INTERVAL '10' YEAR(2)                      AS year_3,    -- 10年
    ts + INTERVAL '1000' YEAR(4)                    AS year_4,    -- 1000年
    ts + INTERVAL '1-03' YEAR TO MONTH              AS month_1,   -- 1年3个月
    ts + INTERVAL '15' MONTH                        AS month_2,   -- 15个月
    ts + INTERVAL '+3' MONTH                        AS month_3,   -- 3个月
    ts + INTERVAL '-3' MONTH                        AS month_4,   -- 3个月
    ts + INTERVAL '+1' DAY                          AS day_1,     -- 1天
    ts + INTERVAL '-1' DAY                          AS day_2,     -- 1天
    ts + INTERVAL '10' DAY(2)                       AS day_3,     -- 10天
    ts + INTERVAL '365' DAY(3)                      AS day_4,     -- 365天
    ts + INTERVAL '-1 03' DAY TO HOUR               AS hour_1,    -- 1天3小时
    ts + INTERVAL '1 03' DAY TO HOUR                AS hour_2,    -- 1天3小时
    ts + INTERVAL '10 03' DAY(2) TO HOUR            AS hour_3,    -- 10天3小时
    ts + INTERVAL '+3' HOUR                         AS hour_4,    -- 3小时
    ts + INTERVAL '-3' HOUR                         AS hour_5,    -- 3小时
    ts + INTERVAL '1 03:20' DAY TO MINUTE           AS minute_1,  -- 1天3小时20分钟
    ts + INTERVAL '-1 03:20' DAY TO MINUTE          AS minute_2,  -- 1天3小时20分钟
    ts + INTERVAL '3:20' HOUR TO MINUTE             AS minute_3,  -- 3小时20分钟
    ts + INTERVAL '-3:20' HOUR TO MINUTE            AS minute_4,  -- 3小时20分钟
    ts + INTERVAL '+20' MINUTE                      AS minute_5,  -- 20分钟
    ts + INTERVAL '-20' MINUTE                      AS minute_6,  -- 20分钟
    ts + INTERVAL '1 03:20:15' DAY TO SECOND        AS second_1,  -- 1天3小时20分钟15秒
    ts + INTERVAL '1 03:20:15.111' DAY TO SECOND(3) AS second_2,  -- 1天3小时20分钟15秒111毫秒
    ts + INTERVAL '3:20:15' HOUR TO SECOND          AS second_3,  -- 3小时20分钟15秒
    ts + INTERVAL '3:20:15.111' HOUR TO SECOND(3)   AS second_4,  -- 3小时20分钟15秒111毫秒
    ts + INTERVAL '20:15' MINUTE TO SECOND          AS second_5,  -- 20分钟15秒
    ts + INTERVAL '20:15.111' MINUTE TO SECOND(3)   AS second_6,  -- 20分钟15秒111毫秒
    ts + INTERVAL '15' SECOND                       AS second_7,  -- 15秒
    ts + INTERVAL '-15' SECOND                      AS second_8,  -- 15秒
    ts + INTERVAL '15.111' SECOND(3)                AS second_9   -- 15秒111毫秒
FROM (
  -- 2022-05-01 12:10:15.456
  SELECT TO_TIMESTAMP_LTZ(1651378215456, 3) AS ts
);