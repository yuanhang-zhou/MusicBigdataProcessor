package org.example;

import org.apache.hadoop.conf.Configuration;
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
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

public class PlayCountProcessor {

    // Mapper 类，用于处理用户信息
    public static class PlayCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split(",");

            if (parts.length == 3) {
                String songId = parts[1];
                int playCount = Integer.parseInt(parts[2]);
                context.write(new Text(songId), new IntWritable(playCount));
            }
        }
    }

    // Reducer 类，用于汇总每首歌曲的播放次数
    public static class PlayCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int totalPlays = 0;
            for (IntWritable value : values) {
                totalPlays += value.get();
            }
            context.write(key, new IntWritable(totalPlays));
        }
    }

    // 主方法，配置和运行 MapReduce 任务，同时找出播放次数最高的前10首歌曲
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Song Play Count");
        job.setJarByClass(PlayCountProcessor.class);

        job.setMapperClass(PlayCountMapper.class);
        job.setReducerClass(PlayCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // 设置输入输出路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 完成并退出
        job.waitForCompletion(true);
        
        //建立songid与songtitle的哈希表
        Path songPath = new Path("songs.txt");
        BufferedReader readerSong = 
        new BufferedReader(new InputStreamReader(songPath.getFileSystem(conf).open(songPath)));
        Map<String, String> songIdToTitle = new HashMap<>();

        String lineSong;
        while ((lineSong = readerSong.readLine()) != null) {
            String[] parts = lineSong.split(",");
            String songId = parts[0];
            String songTitle = parts[2];
            songIdToTitle.put(songId, songTitle);

        }
        readerSong.close();

        // 读取 Reducer 输出，找出播放次数最高的前10首歌曲
        Path outputPath = new Path(args[1] + "/part-r-00000");
        BufferedReader reader = new BufferedReader(new InputStreamReader(outputPath.getFileSystem(conf).open(outputPath)));
        Map<String, Integer> songPlayCounts = new HashMap<>();

        String line;
        while ((line = reader.readLine()) != null) {
            String[] parts = line.split("\t");
            if (parts.length == 2) {
                String songId = parts[0];
                int playCount = Integer.parseInt(parts[1]);
                songPlayCounts.put(songId, playCount);
            }
        }
        reader.close();

        // 使用优先队列找出前10首歌曲
        PriorityQueue<Map.Entry<String, Integer>> topSongs = new PriorityQueue<>((a, b) -> b.getValue() - a.getValue());
        topSongs.addAll(songPlayCounts.entrySet());

        Path outputPathTop10 = new Path("task21.txt");
        BufferedWriter writer = 
        new BufferedWriter(new OutputStreamWriter(outputPathTop10.getFileSystem(conf).create(outputPathTop10, true)));

        for (int i = 0; i < 10 && !topSongs.isEmpty(); i++) {
            Map.Entry<String, Integer> entry = topSongs.poll();
            String songTitle = songIdToTitle.get(entry.getKey());
            writer.write(songTitle+","+entry.getValue());
            writer.newLine();
        }
        writer.close();
    }
}
