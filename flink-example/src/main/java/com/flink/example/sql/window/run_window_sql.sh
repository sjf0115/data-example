#!/usr/bin/env bash

# 1. 进入当前 SQL 文件的路径下
cd /Users/wy/study/code/data-example/flink-example/src/main/java/com/flink/example/sql/window

# 2. 在当前文件目录下执行提交SQL命令:
sql-client.sh embedded -f tvf.sql