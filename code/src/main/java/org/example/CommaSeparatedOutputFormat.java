package org.example;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CommaSeparatedOutputFormat extends FileOutputFormat<Text, Text> {

    @Override
    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        Path outputPath = getDefaultWorkFile(job, "");
        FileSystem fs = outputPath.getFileSystem(job.getConfiguration());
        FSDataOutputStream fileOut = fs.create(outputPath, false);
        return new CommaSeparatedRecordWriter(fileOut);
    }

    public static class CommaSeparatedRecordWriter extends RecordWriter<Text, Text> {
        private final FSDataOutputStream out;

        public CommaSeparatedRecordWriter(FSDataOutputStream out) {
            this.out = out;
        }

        @Override
        public void write(Text key, Text value) throws IOException {
            out.writeBytes(key.toString() + "," + value.toString() + "\n");
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException {
            out.close();
        }
    }
}
