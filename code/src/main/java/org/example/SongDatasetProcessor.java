package org.example;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class SongDatasetProcessor {
    // Mapper 类，用于读取 HDF5 文件并提取歌曲元数据
    public static class SongMetadataMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
            String line = value.toString();
            String[] fields = line.split(",", 2);
            String songId = fields[0];
            String others = fields[1];
                context.write(new Text(songId), new Text(others));
            }
    }

    // Reduce 类，不需要做处理，直接输出
    public static class SongMetadataReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }

    public static void addRecursiveInputPath(Job job, Path path) throws IOException {
        Configuration conf = job.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(path, true); // 第二个参数设为 true 以递归遍历
        while (fileStatusListIterator.hasNext()) {
            LocatedFileStatus fileStatus = fileStatusListIterator.next();
            if (fileStatus.isFile() && fileStatus.getPath().toString().endsWith(".h5")) {
                FileInputFormat.addInputPath(job, fileStatus.getPath());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Song Dataset Processing");
        job.setJarByClass(SongDatasetProcessor.class);

        job.setMapperClass(SongMetadataMapper.class);
        job.setReducerClass(SongMetadataReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(HDF5InputFormat.class);
        job.setOutputFormatClass(CommaSeparatedOutputFormat.class);

        // 设置输入输出路径
        addRecursiveInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 完成并退出
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
