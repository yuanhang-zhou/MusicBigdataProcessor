package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

//TO BE FIXED
public class Task4 {

    public static class GenreWordMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text genre = new Text();
        private Text wordCount = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            genre.set(fileName.substring(0, fileName.indexOf('.')));

            String line = value.toString();
            String[] parts = line.split(",");
            if (parts.length == 2) {
                wordCount.set(parts[0] + "," + parts[1]);
                context.write(genre, wordCount);
            }
        }
    }

    public static class GenreWordReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Integer> wordCountMap = new HashMap<>();
            for (Text value : values) {
                String[] parts = value.toString().split(",");
                String word = parts[0];
                int count = Integer.parseInt(parts[1]);
                wordCountMap.put(word, wordCountMap.getOrDefault(word, 0) + count);
            }

            List<Map.Entry<String, Integer>> wordCountList = new ArrayList<>(wordCountMap.entrySet());
            wordCountList.sort((a, b) -> b.getValue().compareTo(a.getValue()));

            int topN = Math.min(100, wordCountList.size());
            for (int i = 0; i < topN; i++) {
                double weight = 10.0 * (topN - i) / topN;
                context.write(key, new Text(wordCountList.get(i).getKey() + "," + weight));
            }
        }
    }

    public static class TrackMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text trackId = new Text();
        private Text genreOrLyrics = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts;

            if (line.contains("<SEP>")) {
                // unique_tracks.txt
                parts = line.split("<SEP>");
                trackId.set(parts[0]);
                context.write(trackId, new Text("TRACK\t" + parts[0]));
            } else if (line.contains(",")) {
                // lyrics.txt
                parts = line.split(",", 2);
                if (parts.length == 2) {
                    trackId.set(parts[0]);
                    genreOrLyrics.set("LYRICS\t" + parts[1]);
                    context.write(trackId, genreOrLyrics);
                }
            }
        }
    }

    public static class TrackReducer extends Reducer<Text, Text, Text, Text> {
        private Map<String, Map<String, Double>> genreWordWeights = new HashMap<>();
        private Map<String, Map<String, Integer>> lyricsMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Path path = new Path("temp_output/part-r-00000");
            BufferedReader reader = new BufferedReader(new InputStreamReader(FileSystem.get(context.getConfiguration()).open(path)));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                if (parts.length == 2) {
                    String genre = parts[0];
                    String[] wordWeight = parts[1].split(",");
                    String word = wordWeight[0];
                    double weight = Double.parseDouble(wordWeight[1]);

                    genreWordWeights.putIfAbsent(genre, new HashMap<>());
                    genreWordWeights.get(genre).put(word, weight);
                }
            }
            reader.close();
        }
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String trackId = key.toString();
            Map<String, Integer> wordCounts = new HashMap<>();
            int out=0;
            for (Text val : values) {
                String[] parts = val.toString().split("\t");
                if (parts[0].equals("LYRICS")) {
                    StringTokenizer tokenizer = new StringTokenizer(parts[1].substring(1, parts[1].length() - 1), ",");
                    while (tokenizer.hasMoreTokens()) {
                        String[] wordCount = tokenizer.nextToken().split(":");
                        wordCounts.put(wordCount[0].substring(1), Integer.parseInt(wordCount[1].substring(0, wordCount[1].length() - 1)));
                    }
                    lyricsMap.put(trackId, wordCounts);
                } else if (parts[0].equals("TRACK")) {
                    out=1;
                    // Do nothing for now
                }
            }

            if (!wordCounts.isEmpty()&&out==1) {
                String bestGenre = null;
                double bestScore = Double.NEGATIVE_INFINITY;

                for (Map.Entry<String, Map<String, Double>> entry : genreWordWeights.entrySet()) {
                    String genre = entry.getKey();
                    Map<String, Double> wordWeights = entry.getValue();
                    double score = 0.0;

                    for (Map.Entry<String, Integer> wordEntry : wordCounts.entrySet()) {
                        String word = wordEntry.getKey();
                        int count = wordEntry.getValue();
                        score += wordWeights.getOrDefault(word, 0.0) * count;
                    }

                    if (score > bestScore) {
                        bestScore = score;
                        bestGenre = genre;
                    }
                }

                if (bestGenre != null) {
                    context.write(new Text(trackId), new Text(bestGenre));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        // Step 1: Calculate genre word weights
        Path tempOutputPath = new Path("temp_output");
        if (fs.exists(tempOutputPath)) {
            fs.delete(tempOutputPath, true);
        }

        Job job1 = Job.getInstance(conf, "Genre Word Count");
        job1.setJarByClass(Task4.class);
        job1.setMapperClass(GenreWordMapper.class);
        job1.setReducerClass(GenreWordReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, tempOutputPath);

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // Step 2: Classify tracks
        Job job2 = Job.getInstance(conf, "Song Genre Classifier");
        job2.setJarByClass(Task4.class);
        job2.setMapperClass(TrackMapper.class);
        job2.setReducerClass(TrackReducer.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job2, new Path("unique_tracks.txt")); // 输入目录包含 unique_tracks.txt
        FileInputFormat.addInputPath(job2, new Path("lyrics.txt")); // 输入目录包含 lyrics.txt
        FileOutputFormat.setOutputPath(job2, new Path(args[3])); // 输出目录

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
