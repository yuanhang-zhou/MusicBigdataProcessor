package org.example;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtils;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.category.DefaultCategoryDataset;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class HistogramGenerator {

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: HistogramGenerator <input file> <output file>");
            System.exit(-1);
        }

        String inputFile = args[0];
        String outputFile = args[1];

        try {
            CategoryDataset dataset = createDataset(inputFile);
            JFreeChart barChart = ChartFactory.createBarChart(
                    "Song Duration Distribution",
                    "Duration Range",
                    "Number of Songs",
                    dataset,
                    PlotOrientation.VERTICAL,
                    true, true, false);

            int width = 1000;
            int height = 600;
            File barChartFile = new File(outputFile);
            ChartUtils.saveChartAsPNG(barChartFile, barChart, width, height);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static CategoryDataset createDataset(String inputFile) throws IOException {
        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        BufferedReader reader = new BufferedReader(new FileReader(inputFile));
        String line;
        List<RangeCount> rangeCounts = new ArrayList<>();
        
        while ((line = reader.readLine()) != null) {
            String[] parts = line.split(",");
            String range = parts[0];
            int count = Integer.parseInt(parts[1]);
            rangeCounts.add(new RangeCount(range, count));
        }
        reader.close();

        // 对区间进行排序
        Collections.sort(rangeCounts, new Comparator<RangeCount>() {
            @Override
            public int compare(RangeCount o1, RangeCount o2) {
                String[] range1 = o1.range.split("-");
                String[] range2 = o2.range.split("-");
                int start1 = Integer.parseInt(range1[0].replace("[", ""));
                int start2 = Integer.parseInt(range2[0].replace("[", ""));
                return Integer.compare(start1, start2);
            }
        });

        for (RangeCount rc : rangeCounts) {
            dataset.addValue(rc.count, "Songs", rc.range);
        }

        return dataset;
    }

    private static class RangeCount {
        String range;
        int count;

        RangeCount(String range, int count) {
            this.range = range;
            this.count = count;
        }
    }
}
