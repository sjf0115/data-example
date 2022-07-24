package com.hadoop.example.input;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * 功能：使用 文件模式 GlobStatus 过滤
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/7/24 下午10:31
 */
public class GlobStatusExample extends Configured implements Tool {
    public static class WordCountMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, IntWritable> {
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

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("./xxxx <input> <output>");
            System.exit(1);
        }
        // /2007/*/*
        String inputPath = args[0];
        String outputPath = args[1];

        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf);

        job.setJobName("GlobStatusExample");
        job.setJarByClass(GlobStatusExample.class);
        // Map 输出 Key 格式
        job.setMapOutputKeyClass(Text.class);
        // Map 输出 Value 格式
        job.setMapOutputValueClass(IntWritable.class);
        // Mapper 类
        job.setMapperClass(WordCountMapper.class);
        // 输入路径 使用文件模式 GlobStatus 过滤
        FileSystem fileSystem = FileSystem.get(conf);
        FileStatus[] fileStatuses = fileSystem.globStatus(new Path(inputPath));
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
        int result = ToolRunner.run(new Configuration(), new GlobStatusExample(), args);
        System.exit(result);
    }
}
