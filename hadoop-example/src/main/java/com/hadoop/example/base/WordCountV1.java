package com.hadoop.example.base;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * 功能：WordCount V1
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/6/30 下午10:17
 */
public class WordCountV1 extends Configured implements Tool {

    public static class WordCountMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                output.collect(word, one);
            }
        }
    }

    public static class WordCountReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
        }
    }

    public int run(String[] args) throws Exception {
        JobConf conf = new JobConf(getConf(), WordCountV1.class);
        conf.setJobName("WordCount");
        // 输出 Key 格式
        conf.setOutputKeyClass(Text.class);
        // 输出 Value 格式
        conf.setOutputValueClass(IntWritable.class);
        // Mapper 类
        conf.setMapperClass(WordCountMapper.class);
        // Combiner 类
        conf.setCombinerClass(WordCountReducer.class);
        // Reducer 类
        conf.setReducerClass(WordCountReducer.class);
        // Map Task 个数
        conf.setNumMapTasks(2);
        // Reduce Task 个数
        conf.setNumReduceTasks(2);
        // 输入路径
        FileInputFormat.setInputPaths(conf, args[0]);
        // 输出路径
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        JobClient.runJob(conf);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordCountV1(), args);
        System.exit(res);
    }
}
