package com.hadoop.example.input;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * 功能：PathFilter 示例
 * 作者：SmartSi
 * CSDN博客：https://blog.csdn.net/sunnyyoona
 * 公众号：大数据生态
 * 日期：2022/8/1 下午11:19
 */
public class PathFilterExample extends Configured implements Tool {
    public static class WordCountMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {
        private Text word = new Text();
        private Text path = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 路径
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String pathName = fileSplit.getPath().getName();
            path.set(pathName);
            // 单词
            String line = value.toString();
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(path, word);
            }
        }
    }

    public int run(String[] args) throws Exception {
        String inputPath = "/data/word-count/input/2007/*/*";
        String excludePathRegex = "^.*/2007/12/31$";
        String outputPath = "/data/word-count/output/v1";

        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf);

        job.setJobName("PathFilterExample");
        job.setJarByClass(PathFilterExample.class);
        // Map 输出 Key 格式
        job.setMapOutputKeyClass(Text.class);
        // Map 输出 Value 格式
        job.setMapOutputValueClass(Text.class);
        // Mapper 类
        job.setMapperClass(WordCountMapper.class);
        // 输入路径 使用 PathFilter 过滤
        FileSystem fileSystem = FileSystem.get(conf);
        FileStatus[] fileStatuses = fileSystem.globStatus(
                // 初始路径
                new Path(inputPath),
                // 过滤路径
                new RegexExcludePathFilter(excludePathRegex)
        );
        for (FileStatus status : fileStatuses) {
            Path path = status.getPath();
            FileInputFormat.addInputPath(job, path);
        }
        // 输出路径
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new PathFilterExample(), args);
        System.exit(result);
    }

    // 排除满足正则表达式路径过滤器
    private static class RegexExcludePathFilter implements PathFilter {
        private final String regex;
        public RegexExcludePathFilter(String regex) {
            this.regex = regex;
        }
        @Override
        public boolean accept(Path path) {
            return !path.toString().matches(regex);
        }
    }
}
