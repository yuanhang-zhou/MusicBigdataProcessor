package org.example;

import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Filter {

    public static void main(String[] args) throws Exception {
        /*if (args.length < 5) {
            System.err.println("Usage: SongDatasetProcessor <metadata file> <lyrics file> <genres file> <users file> <output dir>");
            System.exit(-1);
        }*/

        String metadataFile = "ptest/songdata/songs.txt";
        String lyricsFile = "ptest/songdata/lyrics.txt";
        String genresFile = "ptest/songdata/genres.txt";
        String usersFile = "ptest/songdata/users.txt";
        String outputDir = "ptest/filter";

        Set<String> trackIds = new HashSet<>();
        Set<String> songIds = new HashSet<>();

        // 读取元数据文件并存储 track_id 和 song_id
        try (BufferedReader br = new BufferedReader(new FileReader(metadataFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length >= 2) {
                    trackIds.add(parts[1]);
                    songIds.add(parts[0]);
                }
            }
        }

        // 过滤歌词信息文件
        filterFile(lyricsFile, outputDir + "/lyrics.txt", trackIds, 0);

        // 过滤流派信息文件
        filterFile(genresFile, outputDir + "/genres.txt", trackIds, 0);

        // 过滤用户信息文件
        filterFile(usersFile, outputDir + "/users.txt", songIds, 1);
    }

    private static void filterFile(String inputFile, String outputFile, Set<String> validIds, int idIndex) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(inputFile));
             BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile))) {

            String line;
            while ((line = br.readLine()) != null) {

                String[] parts = line.split(",");
                if (parts.length > idIndex && validIds.contains(parts[idIndex])) {
                    System.out.println(line);
                    bw.write(line);
                    bw.newLine();
                }
            }
        }
    }
}
