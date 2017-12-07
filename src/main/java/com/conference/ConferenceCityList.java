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
 * Reduce input dataset down to list of cities for each conference
 * output will be tab-separated <conference> <list of distinct cities>
 */
public class ConferenceCityList {
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "conferences");
    job.setJarByClass(ConferenceCityList.class);
    job.setMapperClass(ConferenceCityList.CityMapper.class);
    job.setCombinerClass(ConferenceCityList.ConfCityReducer.class);
    job.setReducerClass(ConferenceCityList.ConfCityReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
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
   */
  public static class CityMapper extends Mapper<Object, Text, Text, Text> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      List<String> columnValues = new ArrayList<>(Arrays.asList(value.toString().split("\t")));

      String conferenceName = columnValues.get(0);
      int lastSpaceBeforeYear = conferenceName.lastIndexOf(' ');
      if (lastSpaceBeforeYear > -1) {
        conferenceName = conferenceName.substring(0, lastSpaceBeforeYear);
      }
      context.write(new Text(conferenceName), new Text(columnValues.get(2)));
    }
  }

  /**
   * Reducer
   * Concatenate the values for each key to determine list of cities per conference
   * The list will be tab-separated
   */
  public static class ConfCityReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      List<String> distinctCities = new ArrayList<>();
      for (Text value : values) {
        List<String> cities = new ArrayList<>(Arrays.asList(String.valueOf(value).split("\t")));
        cities.forEach(city -> {
          if (!distinctCities.contains(city)) {
            distinctCities.add(String.valueOf(city));
          }
        });
      }
      context.write(key, new Text(distinctCities.stream().collect(Collectors.joining("\t"))));
    }

  }
}
