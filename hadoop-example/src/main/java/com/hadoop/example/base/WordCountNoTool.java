package com.hadoop.example.base;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * 功能：WordCount 不实现 Tool 接口
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/7/3 上午10:18
 */
public class WordCountNoTool {
    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
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

    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable intWritable : values){
                sum += intWritable.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    // 启动 MapReduce 作业
    public boolean run(String[] args) throws Exception {
        // 参数解析
        int reduceTask = 0;
        List<String> other_args = new ArrayList<String>();
        for(int i=0; i < args.length; ++i) {
            try {
                if ("-r".equals(args[i])) {
                    reduceTask = Integer.parseInt(args[++i]);
                } else {
                    other_args.add(args[i]);
                }
            } catch (NumberFormatException except) {
                System.out.println("ERROR: Integer expected instead of " + args[i]);
            } catch (ArrayIndexOutOfBoundsException except) {
                System.out.println("ERROR: Required parameter missing from " + args[i-1]);
            }
        }
        if (other_args.size() != 2) {
            System.err.println("./xxxx [-r reduceTaskNum] <input> <output>");
            System.exit(1);
        }
        String inputPaths = other_args.get(0);
        String outputPath = other_args.get(1);

        // 作业配置
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJobName("WordCountNoTool");
        job.setJarByClass(WordCountV2.class);
        // Map 输出 Key 格式
        job.setMapOutputKeyClass(Text.class);
        // Map 输出 Value 格式
        job.setMapOutputValueClass(IntWritable.class);
        // Reduce 输出 Key 格式
        job.setOutputKeyClass(Text.class);
        // Reduce 输出 Value 格式
        job.setOutputValueClass(IntWritable.class);
        // 设置 Reduce Task 个数
        if (reduceTask > 0) {
            job.setNumReduceTasks(reduceTask);
        }
        // Mapper 类
        job.setMapperClass(WordCountMapper.class);
        // Combiner 类
        job.setCombinerClass(WordCountReducer.class);
        // Reducer 类
        job.setReducerClass(WordCountReducer.class);
        // 输入路径
        FileInputFormat.setInputPaths(job, inputPaths);
        // 输出路径
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        boolean success = job.waitForCompletion(true);
        return success;
    }

    public static void main(String[] args) throws Exception {
        WordCountNoTool wordCountNoTool = new WordCountNoTool();
        boolean success = wordCountNoTool.run(args);
        System.exit(success ? 0 : 1);
    }
}
