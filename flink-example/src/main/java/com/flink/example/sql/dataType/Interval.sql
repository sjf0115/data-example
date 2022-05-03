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
