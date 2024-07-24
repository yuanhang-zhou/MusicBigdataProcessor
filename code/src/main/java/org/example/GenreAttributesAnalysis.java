package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class GenreAttributesAnalysis {

    public static class GenreMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Map<String, String> genreMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
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

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] elements = line.split(",");

            if (elements.length >= 12) {
                String trackId = elements[1];
                String genre = genreMap.get(trackId);
                if (genre != null) {
                    String energy = elements[7];   // 第8个元素
                    String tempo = elements[8];    // 第9个元素
                    String loudness = elements[9]; // 第10个元素
                    String duration = elements[10]; // 第11个元素
                    String danceability = elements[11]; // 第12个元素

                    context.write(new Text(genre), new Text(energy + "," + tempo + "," + loudness + "," + duration + "," + danceability));
                }
            }
        }
    }

    public static class GenreReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            double totalEnergy = 0;
            double totalTempo = 0;
            double totalLoudness = 0;
            double totalDuration = 0;
            double totalDanceability = 0;

            for (Text value : values) {
                String[] attributes = value.toString().split(",");
                if (attributes.length == 5) {
                    totalEnergy += Double.parseDouble(attributes[0]);
                    totalTempo += Double.parseDouble(attributes[1]);
                    totalLoudness += Double.parseDouble(attributes[2]);
                    totalDuration += Double.parseDouble(attributes[3]);
                    totalDanceability += Double.parseDouble(attributes[4]);
                    count++;
                }
            }

            if (count > 0) {
                double avgEnergy = totalEnergy / count;
                double avgTempo = totalTempo / count;
                double avgLoudness = totalLoudness / count;
                double avgDuration = totalDuration / count;
                double avgDanceability = totalDanceability / count;

                context.write(key, new Text(avgEnergy + "," + avgTempo + "," + avgLoudness + "," + avgDuration + "," + avgDanceability));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Genre Attributes Analysis");
        job.setJarByClass(GenreAttributesAnalysis.class);

        job.setMapperClass(GenreMapper.class);
        job.setReducerClass(GenreReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);

        if (success) {
            // 读取输出文件并绘制图表
            Path outputPath = new Path(args[1] + "/part-r-00000");
            FileSystem fs = FileSystem.get(conf);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(outputPath)));
            Map<String, double[]> genreAverages = new HashMap<>();

            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split("\t");
                if (parts.length == 2) {
                    String genre = parts[0];
                    String[] averages = parts[1].split(",");

                    double[] values = new double[5];
                    for (int i = 0; i < 5; i++) {
                        values[i] = Double.parseDouble(averages[i]);
                    }
                    genreAverages.put(genre, values);
                }
            }
            br.close();

            // 绘制直线图
            int width = 800, height = 600;
            BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
            Graphics2D g2d = image.createGraphics();
            g2d.setColor(Color.WHITE);
            g2d.fillRect(0, 0, width, height);
            g2d.setColor(Color.BLACK);
            g2d.drawString("Genre Attributes Averages", 10, 20);

            int margin = 50;
            int graphWidth = width - 2 * margin;
            int graphHeight = height - 2 * margin;
            double maxY = genreAverages.values().stream()
                    .flatMapToDouble(Arrays::stream)
                    .max()
                    .orElse(1);
            double genreWidth = (double) graphWidth / genreAverages.size();

            int genreIndex = 0;
            for (Map.Entry<String, double[]> entry : genreAverages.entrySet()) {
                String genre = entry.getKey();
                double[] averages = entry.getValue();

                for (int i = 0; i < averages.length; i++) {
                    int x = (int) (margin + genreWidth * genreIndex + genreWidth / 2);
                    int y = (int) (height - margin - (averages[i] / maxY) * graphHeight);
                    g2d.fillOval(x - 5, y - 5, 10, 10);
                }
                genreIndex++;
            }

            // 输出PNG文件
            Path histogramPath = new Path(args[1] + "/task25.png");
            File outputFile = new File("task25.png");
            ImageIO.write(image, "png", outputFile);
            fs.copyFromLocalFile(new Path(outputFile.getAbsolutePath()), histogramPath);
        }

        System.exit(success ? 0 : 1);
    }
}
