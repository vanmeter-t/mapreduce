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
 * Reduce input dataset down to count of conferences per location
 * output will be tab-separated <location> <count of conferences>
 */
public class ConferenceCount {
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "conferences");
    job.setJarByClass(ConferenceCount.class);
    job.setMapperClass(ConferenceCount.CityMapper.class);
    job.setCombinerClass(ConferenceCount.ConfCityReducer.class);
    job.setReducerClass(ConferenceCount.ConfCityReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

  /**
   * Mapper
   * for each record: tab-separated columns setup as Name (0), Description (1), Location (2)
   * extract the Location (2) and assign initial count of 1 for mapping
   */
  public static class CityMapper extends Mapper<Object, Text, Text, IntWritable> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      List<String> columnValues = new ArrayList<>(Arrays.asList(value.toString().split("\t")));
      context.write(new Text(columnValues.get(2)), new IntWritable(1));
    }
  }

  /**
   * Reducer
   * Sum the values for each key to determine total count of conferences per location
   * Sort the map by value in the cleanup function
   */
  public static class ConfCityReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

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
}
