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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class Preference {

    public static class UserMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Map<String, String> songArtistMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Read songs.txt to map song_id to artist_id
            Path songPath = new Path("songs.txt");
            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(songPath)));
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length > 4) {
                    String songId = parts[0]; // song_id
                    String artistId = parts[4]; // artist_id
                    songArtistMap.put(songId, artistId);
                }
            }
            br.close();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split(",");
            if (parts.length >= 3) {
                String userId = parts[0]; // user_id
                String songId = parts[1]; // song_id
                int playCount = Integer.parseInt(parts[2]); // play_count

                String artistId = songArtistMap.get(songId);
                if (artistId != null) {
                    // Emit userId and play count
                    context.write(new Text(userId), new IntWritable(playCount));

                    // Emit artistId and play count
                    context.write(new Text(artistId), new IntWritable(playCount));
                }
            }
        }
    }

    public static class UserArtistReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private Map<String, Integer> userPlayCount = new HashMap<>();
        private Map<String, Integer> artistPlayCount = new HashMap<>();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }

            // Determine if key is user or artist
            if (!key.toString().startsWith("AR")) {
                userPlayCount.put(key.toString(), sum);
            } else {
                artistPlayCount.put(key.toString(), sum);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Find max user and artist
            String maxUserId = null;
            int maxUserCount = 0;
            for (Map.Entry<String, Integer> entry : userPlayCount.entrySet()) {
                if (entry.getValue() > maxUserCount) {
                    maxUserCount = entry.getValue();
                    maxUserId = entry.getKey();
                }
            }
            String maxArtistId = null;
            int maxArtistCount = 0;
            for (Map.Entry<String, Integer> entry : artistPlayCount.entrySet()) {
                if (entry.getValue() > maxArtistCount) {
                    maxArtistCount = entry.getValue();
                    maxArtistId = entry.getKey();
                }
            }
            // Emit results
            if (maxUserId != null && maxArtistId != null) {
                context.write(new Text(maxUserId + "," + maxArtistId), new IntWritable(maxUserCount));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "User Artist Play Count");
        job.setJarByClass(Preference.class);
        job.setMapperClass(UserMapper.class);
        job.setReducerClass(UserArtistReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0])); // Input path for users.txt
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // Output path for task22.txt

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

