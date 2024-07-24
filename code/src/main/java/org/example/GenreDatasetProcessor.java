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

public class GenreDatasetProcessor {

    // Mapper 类，用于处理流派信息
    public static class GenreMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            // 跳过注释行
            if (line.startsWith("#")) {
                return;
            }

            // 普通行的格式：track_id,genre
            String[] parts = line.split("\\t");
            String trackId = parts[0];
            String genre = parts[1];

            context.write(new Text(trackId), new Text(genre));
        }
    }

    // Reducer 类，用于将 Mapper 的输出直接写入结果
    public static class GenreReducer extends Reducer<Text, Text, Text, Text> {
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

        Job job = Job.getInstance(conf, "Genre Processing");
        job.setJarByClass(GenreDatasetProcessor.class);

        job.setMapperClass(GenreMapper.class);
        job.setReducerClass(GenreReducer.class);

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
