package com.conference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Reduce input dataset down to count of conferences per year for each city
 * output will be tab-separated <conference> <year> <count of conferences>
 */
public class CityYearCountList {
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "conferences");
    job.setJarByClass(CityYearCountList.class);
    job.setMapperClass(CityYearCountList.CityMapper.class);
    job.setCombinerClass(CityYearCountList.YearCountCityReducer.class);
    job.setReducerClass(CityYearCountList.YearCountCityReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

  /**
   * Mapper
   * for each record: tab-separated columns setup as Name (0), Description (1), Location (2)
   * extract the Conference Name (2) without the Year, and extract the Location (0)
   * extracting just the Conference Name will split on the last found <space> character
   * assuming the following text is the year to be removed
   * if a year value is not found, the data item will be skipped
   */
  public static class CityMapper extends Mapper<Object, Text, Text, IntWritable> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      List<String> columnValues = new ArrayList<>(Arrays.asList(value.toString().split("\t")));
      String conferenceName = columnValues.get(0);
      int lastSpaceBeforeYear = conferenceName.lastIndexOf(' ');
      if (lastSpaceBeforeYear > -1) {
        String year = conferenceName.substring(lastSpaceBeforeYear + 1, conferenceName.length()).trim();
        String finalKey = String.format("%s\t%s", columnValues.get(2), year);
        context.write(new Text(finalKey), new IntWritable(1));
      }
    }
  }

  /**
   * Reducer
   * Sum the values for each key to determine total count of conferences per location/year combination
   * Sort the map by value in the cleanup function
   */
  public static class YearCountCityReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    public Map<String, Integer> map = new LinkedHashMap<>();

    public static Map<String, Integer> sortByValue(Map<String, Integer> map) {
      return map.entrySet()
        .stream()
        .sorted(Map.Entry.comparingByValue())
        .collect(Collectors.toMap(
          Map.Entry::getKey,
          Map.Entry::getValue,
          (e1, e2) -> e1,
          LinkedHashMap::new
        ));
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable value : values) {
        sum += value.get();
      }
      map.put(key.toString(), sum);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      sortByValue(map).forEach((k, v) -> {
        try {
          context.write(new Text(k), new IntWritable(v));
        } catch (Exception ex) {
          throw new RuntimeException(ex);
        }
      });
    }

  }

  /**
   * Reducer
   * Concatenate the values for each key to generate a list of number of conferences each year for the cities
   * The list will be tab-separated
   */
  public static class ConfCityReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      Map<String, Integer> distinctYears = new LinkedHashMap<>();
      for (Text value : values) {
        List<String> years = new ArrayList<>(Arrays.asList(String.valueOf(value).split("\t")));
        years.forEach(yearCount -> {
          String year = yearCount.split(":")[0];
          Integer yearCt = Integer.parseInt(yearCount.split(":")[1]);
          if (!distinctYears.containsKey(year)) {
            distinctYears.put(year, yearCt);
          } else {
            int count = distinctYears.get(year) + yearCt;
            distinctYears.put(year, count);
          }

        });
      }
      context.write(key, new Text(distinctYears.entrySet().stream()
        .map(e -> String.format("%s:%s", e.getKey(), e.getValue().toString())).collect(Collectors.joining("\t"))));
    }

  }
}
