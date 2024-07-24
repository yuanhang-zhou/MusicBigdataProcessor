package org.example;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

public class SongDurationDistribution {

    public static class DurationMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split(",");
            if (parts.length > 10) {
                try {
                    double duration = Double.parseDouble(parts[10]);
                    String durationRange = getDurationRange(duration);
                    context.write(new Text(durationRange), new IntWritable(1));
                } catch (NumberFormatException e) {
                    e.printStackTrace();
                }
            }
        }

        private String getDurationRange(double duration) {
            if (duration <= 60) return "[0,60]";
            if (duration <= 120) return "[61,120]";
            if (duration <= 180) return "[121,180]";
            if (duration <= 240) return "[181,240]";
            if (duration <= 300) return "[241,300]";
            if (duration <= 360) return "[301,360]";
            if (duration <= 420) return "[361,420]";
            if (duration <= 480) return "[421,480]";
            if (duration <= 540) return "[481,540]";
            return "[541,~)";
        }
    }

    public static class DurationReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Song Duration Distribution");
        job.setJarByClass(SongDurationDistribution.class);

        job.setMapperClass(DurationMapper.class);
        job.setReducerClass(DurationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // 设置输入输出路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 完成并退出
        // 完成并退出
        boolean success = job.waitForCompletion(true);

        if (success) {
            // 使用 Hadoop FileSystem API 读取输出文件
            Path outputPath = new Path(args[1] + "/part-r-00000");
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(outputPath)) {
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(outputPath)));
                String line;
                StringBuilder outputContent = new StringBuilder();
                while ((line = br.readLine()) != null) {
                    outputContent.append(line).append("\n");
                }
                br.close();

                // 将内容写入本地文件以便 HistogramGenerator 使用
                String localOutputPath = args[1] + "duration.txt";
                java.nio.file.Files.write(java.nio.file.Paths.get(localOutputPath), outputContent.toString().getBytes());

                String histogramPath = args[1] + "task23.png";
                // 生成直方图
                HistogramGenerator.main(new String[]{java.nio.file.Paths.get(localOutputPath).toString(), histogramPath, "true"});
            } else {
                System.err.println("Output file not found: " + outputPath);
            }
        }

        System.exit(success ? 0 : 1);
    }
}
