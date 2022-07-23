package com.hadoop.example.output;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * 功能：LazyOutputFormat 延迟输出示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/7/23 下午4:54
 */
public class LazyOutputExample extends Configured implements Tool {
    // Mapper
    public static class LazyOutputMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    // Reducer
    public static class LazyOutputReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private MultipleOutputs<Text, IntWritable> multipleOutputs;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // 初始化 MultipleOutputs
            multipleOutputs = new MultipleOutputs<Text, IntWritable>(context);
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable intWritable : values){
                sum += intWritable.get();
            }
            // key 首字母作为基础输出路径
            String baseOutput = StringUtils.substring(key.toString(), 0, 1);
            // 使用 multipleOutputs
            String basePath = StringUtils.lowerCase(baseOutput);
            multipleOutputs.write(key, new IntWritable(sum), basePath);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
        }
    }

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("./xxxx <input> <output>");
            System.exit(1);
        }
        String inputPaths = args[0];
        String outputPath = args[1];

        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf);
        job.setJobName("LazyOutputExample");
        job.setJarByClass(LazyOutputExample.class);
        // Map 输出 Key 格式
        job.setMapOutputKeyClass(Text.class);
        // Map 输出 Value 格式
        job.setMapOutputValueClass(IntWritable.class);
        // Reduce 输出 Key 格式
        job.setOutputKeyClass(Text.class);
        // Reduce 输出 Value 格式
        job.setOutputValueClass(IntWritable.class);
        // Mapper 类
        job.setMapperClass(LazyOutputMapper.class);
        // Reducer 类
        job.setReducerClass(LazyOutputReducer.class);
        // 输入路径
        FileInputFormat.setInputPaths(job, inputPaths);
        // 输出路径
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        // 延迟输出
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new LazyOutputExample(), args);
        System.exit(result);
    }
}
