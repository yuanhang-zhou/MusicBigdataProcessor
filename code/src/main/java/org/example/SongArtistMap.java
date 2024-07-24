package org.example;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SongArtistMap {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: SongMapReduce <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Song Artist ID");

        job.setJarByClass(SongArtistMap.class);
        job.setMapperClass(SongMapper.class);
        job.setReducerClass(SongReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class SongMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");

            if (fields.length == 12) {
                String song_id = fields[0];
                String artist_id = fields[4];
                context.write(new Text(song_id), new Text(artist_id));
            }
        }
    }

    public static class SongReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
                break; // Each song_id has one artist_id
            }
        }
    }
}
