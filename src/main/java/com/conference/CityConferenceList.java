package com.conference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Reduce input dataset down to list of conferences per location
 * output will be tab-separated <location> <list of distinct conferences>
 */
public class CityConferenceList {
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "conferences");
    job.setJarByClass(CityConferenceList.class);
    job.setMapperClass(CityConferenceList.CityMapper.class);
    job.setCombinerClass(CityConferenceList.ConfCityReducer.class);
    job.setReducerClass(CityConferenceList.ConfCityReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

  /**
   * Mapper
   * for each record: tab-separated columns setup as Name (0), Description (1), Location (2)
   * extract the Location (2) and extract the Name (0)
   */
  public static class CityMapper extends Mapper<Object, Text, Text, Text> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      List<String> columnValues = new ArrayList<>(Arrays.asList(value.toString().split("\t")));
      context.write(new Text(columnValues.get(2)), new Text(columnValues.get(0)));
    }
  }

  /**
   * Reducer
   * Concatenate the values for each key to determine list of conferences per location
   * The list will be tab-separated
   */
  public static class ConfCityReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      List<String> distinctConference = new ArrayList<>();
      for (Text value : values) {
        List<String> conferences = new ArrayList<>(Arrays.asList(String.valueOf(value).split("\t")));
        conferences.forEach(conf -> {
          if (!distinctConference.contains(conf)) {
            distinctConference.add(String.valueOf(conf));
          }
        });
      }
      context.write(key, new Text(distinctConference.stream().collect(Collectors.joining("\t"))));
    }

  }
}
