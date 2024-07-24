package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class UserDatasetProcessor {

    // Mapper 类，用于处理用户信息
    public static class UserMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            // 用户信息的格式：user_id \t song_id \t play_count
            String[] parts = line.split("\t");
            if (parts.length != 3) {
                return; // 跳过格式不正确的行
            }

            String userId = parts[0];
            String songId = parts[1];
            String playCount = parts[2];

            context.write(new Text(userId), new Text(songId + "," + playCount));
        }
    }

    // Reducer 类，用于将 Mapper 的输出直接写入结果
    public static class UserReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }

    // 主方法，配置和运行 MapReduce 任务
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "User Processing");
        job.setJarByClass(UserDatasetProcessor.class);

        job.setMapperClass(UserMapper.class);
        job.setReducerClass(UserReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(CommaSeparatedOutputFormat.class);

        // 设置输入输出路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 完成并退出
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
