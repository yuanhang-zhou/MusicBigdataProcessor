package org.example;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import ncsa.hdf.hdf5lib.H5;
import ncsa.hdf.hdf5lib.HDF5Constants;

public class HDF5RecordReader extends RecordReader<LongWritable, Text> {
    private LongWritable key = new LongWritable();
    private Text value = new Text();
    private boolean processed = false;
    private File localTempFile;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) split;
        Configuration conf = context.getConfiguration();
        Path path = fileSplit.getPath();

        // 将 HDFS 文件复制到本地临时文件
        FileSystem fs = path.getFileSystem(conf);
        localTempFile = File.createTempFile("hdf5_temp", ".h5");
        localTempFile.deleteOnExit();

        try (InputStream in = fs.open(path);
             FileOutputStream out = new FileOutputStream(localTempFile)) {
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
            }
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (processed) {
            return false;
        }

        HashMap<String, String> data = new HashMap<>();

        try {
            int file_id = -1;
            int dataset_id = -1;
            int type_id = -1;

            file_id = H5.H5Fopen(localTempFile.getAbsolutePath(), HDF5Constants.H5F_ACC_RDONLY, HDF5Constants.H5P_DEFAULT);
            String[] datasetNames = new String[]{"/metadata/songs", "/analysis/songs", "/musicbrainz/songs"};

            for (String datasetName : datasetNames) {
                dataset_id = H5.H5Dopen(file_id, datasetName, HDF5Constants.H5P_DEFAULT);
                type_id = H5.H5Dget_type(dataset_id);

                byte[] dataBuffer = new byte[H5.H5Tget_size(type_id)];
                H5.H5Dread(dataset_id, type_id, HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL, HDF5Constants.H5P_DEFAULT, dataBuffer);
                int numberOfFields = H5.H5Tget_nmembers(type_id);

                for (int i = 0; i < numberOfFields; i++) {
                    String fieldName = H5.H5Tget_member_name(type_id, i);
                    int fieldType = H5.H5Tget_member_class(type_id, i);
                    long fieldOffset = H5.H5Tget_member_offset(type_id, i);
                    int fieldSize = H5.H5Tget_size(H5.H5Tget_member_type(type_id, i));
                    int index = (int) (fieldOffset);
                    switch (fieldType) {
                        case 0:
                            int intValue = byteArrayToInt(dataBuffer, index, fieldSize);
                            data.put(fieldName, String.valueOf(intValue));
                            break;
                        case 1:
                            double doubleValue = byteArrayToDouble(dataBuffer, index);
                            data.put(fieldName, String.valueOf(doubleValue));
                            break;
                        case 3:
                            String stringValue = byteArrayToString(dataBuffer, index, fieldSize);
                            data.put(fieldName, stringValue);
                            break;
                        default: // skip other types
                            break;
                    }
                }
                H5.H5Dclose(dataset_id);
                H5.H5Tclose(type_id);
            }
            H5.H5Fclose(file_id);

            String songId = data.get("song_id");
            String trackId = data.get("track_id");
            String title = data.get("title");
            String release = data.get("release");
            String artistId = data.get("artist_id");
            String artistName = data.get("artist_name");
            String mode = data.get("mode");
            String energy = data.get("energy");
            String tempo = data.get("tempo");
            String loudness = data.get("loudness");
            String duration = data.get("duration");
            String danceability = data.get("danceability");
            String year = data.get("year");

            StringBuilder sb = new StringBuilder();
            sb.append(trackId).append(",")
                    .append(title).append(",")
                    .append(release).append(",")
                    .append(artistId).append(",")
                    .append(artistName).append(",")
                    .append(mode).append(",")
                    .append(energy).append(",")
                    .append(tempo).append(",")
                    .append(loudness).append(",")
                    .append(duration).append(",")
                    .append(danceability).append(",")
                    .append(year);

            key.set(0); // Assign a dummy key since the file only has one record
            value.set(sb.toString());
            processed = true;
            return true;
        } catch (Exception e) {
            throw new IOException("Error processing HDF5 file", e);
        }
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return processed ? 1.0f : 0.0f;
    }

    @Override
    public void close() throws IOException {
        if (localTempFile != null && localTempFile.exists()) {
            localTempFile.delete();
        }
    }

    private static int byteArrayToInt(byte[] arr, int offset, int size) {
        int value = 0;
        for (int i = 0; i < size; i++) {
            value |= ((int) arr[offset + i] & 0xFF) << (8 * i);
        }
        return value;
    }

    private static double byteArrayToDouble(byte[] arr, int offset) {
        long longBits = byteArrayToLong(arr, offset, 8);
        return Double.longBitsToDouble(longBits);
    }

    private static long byteArrayToLong(byte[] arr, int offset, int length) {
        long value = 0;
        for (int i = 0; i < length; i++) {
            value |= ((long) (arr[offset + i] & 0xff)) << (8 * i);
        }
        return value;
    }

    private static String byteArrayToString(byte[] dataBuffer, int index, int fieldSize) {
        StringBuilder stringValue = new StringBuilder();
        for (int i = 0; i < fieldSize; i++) {
            byte b = dataBuffer[index + i];
            if (b == 0) {
                break; // 遇到终止符时停止读取
            }
            stringValue.append((char) b);
        }
        return stringValue.toString();
    }
}
