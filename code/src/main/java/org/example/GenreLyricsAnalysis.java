package org.example;

import com.kennycason.kumo.CollisionMode;
import com.kennycason.kumo.WordCloud;
import com.kennycason.kumo.font.scale.LinearFontScalar;
import com.kennycason.kumo.palette.ColorPalette;
import com.kennycason.kumo.wordstart.CenterWordStart;
import com.kennycason.kumo.WordFrequency;
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

import java.awt.*;
import java.io.*;
import java.util.*;
import java.util.List;

public class GenreLyricsAnalysis {

    public static class GenreMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Map<String, String> genreMap = new HashMap<>();
        //在setup首先建立trackid和genre的匹配
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Path genrePath = new Path("genres.txt");
            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(genrePath)));
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length == 2) {
                    genreMap.put(parts[0], parts[1]);
                }
            }
            br.close();
        }
        //分析lyrics.txt
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            int firstCommaIndex = line.indexOf(',');
            if (firstCommaIndex != -1) {
                String trackId = line.substring(0, firstCommaIndex);
                String wordCounts = line.substring(firstCommaIndex + 1).trim();
                String genre = genreMap.get(trackId);

                if (genre != null && wordCounts.startsWith("[") && wordCounts.endsWith("]")) {
                    wordCounts = wordCounts.substring(1, wordCounts.length() - 1);
                    String[] wordCountPairs = wordCounts.split("\\),\\(");
                    for (String pair : wordCountPairs) {
                        pair = pair.replace("(", "").replace(")", "");
                        String[] wc = pair.split(":");
                        if (wc.length == 2) {
                            context.write(new Text(genre + "," + wc[0]), new IntWritable(Integer.parseInt(wc[1])));
                        }
                    }
                }
            }
        }
    }

    public static class GenreReducer extends Reducer<Text, IntWritable, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key,  new Text(String.valueOf(sum)));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Genre Lyrics Analysis");
        job.setJarByClass(GenreLyricsAnalysis.class);

        job.setMapperClass(GenreMapper.class);
        job.setReducerClass(GenreReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(CommaSeparatedOutputFormat.class);

        // 设置输入输出路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 完成并退出
        boolean success = job.waitForCompletion(true);

        if (success) {
            // 使用 Hadoop FileSystem API 读取输出文件
            Path outputPath = new Path(args[1] + "/part-r-00000");
            BufferedReader br = new BufferedReader(new InputStreamReader(outputPath.getFileSystem(conf).open(outputPath)));
            String line;
            Map<String, Map<String, Integer>> genreWordCounts = new HashMap<>();
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length == 3) {
                    String genre = parts[0];
                    String word = parts[1];
                    int count = Integer.parseInt(parts[2]);
                    genreWordCounts.putIfAbsent(genre, new HashMap<>());
                    Map<String, Integer> wordCounts = genreWordCounts.get(genre);
                    wordCounts.put(word, wordCounts.getOrDefault(word, 0) + count);
                }
            }
            br.close();

            for (Map.Entry<String, Map<String, Integer>> entry : genreWordCounts.entrySet()) {
                String genre = entry.getKey();
                Map<String, Integer> wordCounts = entry.getValue();
                Path outputPathGenre = new Path(args[1] + "/task24/" + genre + ".txt");

                // 创建HDFS路径并写入文件
                try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(outputPathGenre.getFileSystem(conf).create(outputPathGenre, true)))) {
                    for (Map.Entry<String, Integer> wordCountEntry : wordCounts.entrySet()) {
                        bw.write(wordCountEntry.getKey() + "," + wordCountEntry.getValue());
                        bw.newLine();
                    }
                }
            }

            // 找到歌曲数量最多的流派
            String maxGenre = null;
            int maxCount = 0;
            for (Map.Entry<String, Map<String, Integer>> entry : genreWordCounts.entrySet()) {
                int count = entry.getValue().values().stream().mapToInt(Integer::intValue).sum();
                if (count > maxCount) {
                    maxCount = count;
                    maxGenre = entry.getKey();
                }
            }

            // 生成词云图
            if (maxGenre != null) {
                List<WordFrequency> wordFrequencies = new ArrayList<>();
                Map<String, Integer> wordCounts = genreWordCounts.get(maxGenre);
                for (Map.Entry<String, Integer> entry : wordCounts.entrySet()) {
                    wordFrequencies.add(new WordFrequency(entry.getKey(), entry.getValue()));
                }

                // Configure word cloud
                Dimension dimension = new Dimension(800, 600);
                WordCloud wordCloud = new WordCloud(dimension, CollisionMode.PIXEL_PERFECT);
                wordCloud.setPadding(2);
                wordCloud.setBackgroundColor(Color.WHITE);
                wordCloud.setFontScalar(new LinearFontScalar(10, 50));
                wordCloud.setColorPalette(new ColorPalette(new Color(0x4055F1), new Color(0x408DF1), new Color(0x40AAF1), new Color(0x40C5F1), new Color(0x40D3F1), new Color(0xFFFFFF)));
                wordCloud.setWordStartStrategy(new CenterWordStart());
                wordCloud.build(wordFrequencies);

                // Save word cloud to a local file
                File outputFile = new File("task24.png");
                wordCloud.writeToFile(outputFile.getAbsolutePath());

                // Copy local file to HDFS
                Path histogramPath = new Path(args[1] + "/task24.png");
                FileSystem fs1 = FileSystem.get(conf);
                fs1.copyFromLocalFile(new Path(outputFile.getAbsolutePath()), histogramPath);

            }
        }

        System.exit(success ? 0 : 1);
    }
}

