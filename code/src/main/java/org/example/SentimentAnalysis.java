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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

public class SentimentAnalysis {

    public static class SentimentMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private Map<String, Integer> sentimentMap = new HashMap<>();
        private Set<String> stopwords = new HashSet<>();
        private double positiveWeight = 1.0;
        private double negativeWeight = 1.0;
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Path sentimentPath = new Path("sentiment_train.txt");
            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(sentimentPath)));
            String line;
            int positiveCount = 0;
            int negativeCount = 0;
            
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length == 2) {
                    String trackId = parts[0];
                    String sentiment = parts[1];
                    int sentimentValue = sentiment.equals("positive") ? 1 : (sentiment.equals("negative") ? -1 : 0);
                    sentimentMap.put(trackId, sentimentValue);
                    if (sentimentValue == 1) {
                        positiveCount++;
                    } else if (sentimentValue == -1) {
                        negativeCount++;
                    }
                }
            }
            br.close();
            
            // Adjust weights based on the count of positive and negative samples
            if (positiveCount > negativeCount) {
                negativeWeight = (double) positiveCount / negativeCount;
            } else if (negativeCount > positiveCount) {
                positiveWeight = (double) negativeCount / positiveCount;
            }

            Path stopwordsPath = new Path("stopwords.txt");
            br = new BufferedReader(new InputStreamReader(fs.open(stopwordsPath)));
            while ((line = br.readLine()) != null) {
                stopwords.add(line.trim().toLowerCase());
            }
            br.close();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            int firstCommaIndex = line.indexOf(',');
            if (firstCommaIndex != -1) {
                String trackId = line.substring(0, firstCommaIndex);
                String wordCounts = line.substring(firstCommaIndex + 1).trim();
                Integer sentimentValue = sentimentMap.get(trackId);

                if (sentimentValue != null && wordCounts.startsWith("[") && wordCounts.endsWith("]")) {
                    wordCounts = wordCounts.substring(1, wordCounts.length() - 1);
                    String[] wordCountPairs = wordCounts.split("\\),\\(");

                    int totalWordCount = 0;
                    Map<String, Integer> wordCountMap = new HashMap<>();

                    for (String pair : wordCountPairs) {
                        pair = pair.replace("(", "").replace(")", "");
                        String[] wc = pair.split(":");
                        if (wc.length == 2) {
                            String word = wc[0];
                            int count = Integer.parseInt(wc[1]);
                            if (!stopwords.contains(word)) {
                                wordCountMap.put(word, count);
                                totalWordCount += count;
                            }
                        }
                    }

                    double weightFactor = sentimentValue == 1 ? positiveWeight : negativeWeight;

                    for (Map.Entry<String, Integer> entry : wordCountMap.entrySet()) {
                        String word = entry.getKey();
                        int count = entry.getValue();
                        double weight = (1.0 / totalWordCount) * sentimentValue * weightFactor;
                        context.write(new Text(word), new DoubleWritable(weight));
                    }
                }
            }
        }
    }

    public static class SentimentReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sentimentScore = 0.0;
            for (DoubleWritable value : values) {
                sentimentScore += value.get();
            }
            context.write(key, new DoubleWritable(sentimentScore));
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Sentiment Analysis");
        job.setJarByClass(SentimentAnalysis.class);

        job.setMapperClass(SentimentMapper.class);
        job.setReducerClass(SentimentReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // 设置输入输出路径
        FileInputFormat.addInputPath(job, new Path(args[0])); // lyrics.txt
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);

        if (success) {
            // 读取词汇情感权值文件
            Path outputPath = new Path(args[1] + "/part-r-00000");
            BufferedReader br = new BufferedReader(new InputStreamReader(outputPath.getFileSystem(conf).open(outputPath)));
            String line;
            Map<String, Double> wordSentimentMap = new HashMap<>();
            while ((line = br.readLine()) != null) {
                String[] parts = line.split("\t");
                if (parts.length == 2) {
                    String word = parts[0];
                    double sentimentScore = Double.parseDouble(parts[1]);
                    wordSentimentMap.put(word, sentimentScore);
                }
            }
            br.close();

            // 分析每个track_id的情感
            Path lyricsPath = new Path("lyric1.txt");
            Path resultPath = new Path(args[1] + "/track_sentiment.txt");
            BufferedReader lyricsReader = new BufferedReader(new InputStreamReader(lyricsPath.getFileSystem(conf).open(lyricsPath)));
            BufferedWriter resultWriter = new BufferedWriter(new OutputStreamWriter(resultPath.getFileSystem(conf).create(resultPath, true)));
            while ((line = lyricsReader.readLine()) != null) {
                int firstCommaIndex = line.indexOf(',');
                if (firstCommaIndex != -1) {
                    String trackId = line.substring(0, firstCommaIndex);
                    String wordCounts = line.substring(firstCommaIndex + 1).trim();

                    if (wordCounts.startsWith("[") && wordCounts.endsWith("]")) {
                        double trackSentimentScore = 0.0;
                        wordCounts = wordCounts.substring(1, wordCounts.length() - 1);
                        String[] wordCountPairs = wordCounts.split("\\),\\(");
                        for (String pair : wordCountPairs) {
                            pair = pair.replace("(", "").replace(")", "");
                            String[] wc = pair.split(":");
                            if (wc.length == 2) {
                                String word = wc[0];
                                int count = Integer.parseInt(wc[1]);
                                Double wordSentiment = wordSentimentMap.get(word);
                                if (wordSentiment != null) {
                                    trackSentimentScore += count * wordSentiment;
                                }
                            }
                        }
                        String trackSentiment = trackSentimentScore > 0 ? "positive" : (trackSentimentScore < 0 ? "negative" : "neutral");
                        resultWriter.write(trackId + "," + trackSentiment);
                        resultWriter.newLine();
                    }
                }
            }
            lyricsReader.close();
            resultWriter.close();
        }

        System.exit(success ? 0 : 1);
    }
}
