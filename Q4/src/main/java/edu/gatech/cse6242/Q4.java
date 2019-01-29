package edu.gatech.cse6242;
// the code references tutorial https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#Example:_WordCount_v1.0
import java.io.IOException;
import java.util.StringTokenizer; 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q4 {

  public static class differenceMapper
     extends Mapper<Object, Text, Text, IntWritable>{

    //define integer 1 and -1, once they are inititialize, they cannot be changed & text
    private Text indegree = new Text();
    private Text outdegree = new Text();
    private final static IntWritable positive = new IntWritable(1);
    private final static IntWritable negative = new IntWritable(-1);

  public void map(Object key, Text value, Context context
                  ) throws IOException, InterruptedException {
    
     String word = value.toString();
     String[] wordsplit = word.split("\t");
          outdegree.set(wordsplit[0]);
          indegree.set(wordsplit[1]);
          context.write(outdegree, negative);
          context.write(indegree, positive);
    
  }
}

public static class differenceReducer
     extends Reducer<Text,IntWritable,Text,IntWritable> {
  //define variables result    
  private IntWritable result = new IntWritable();
  public void reduce(Text key, Iterable<IntWritable> values,
                     Context context
                     ) throws IOException, InterruptedException {

    // the sum calculate the nodes degree difference
    int sum = 0;
    int number_get;
    for (IntWritable val : values) {
      number_get = val.get();
      
          sum += number_get; 
    }
    result.set(sum);
    context.write(key, result);
  }
}

  public static class countMapper
     extends Mapper<Object, Text, Text, IntWritable>{
    //define variables
    private Text wordvalue = new Text();
    private IntWritable positive = new IntWritable(1);

  public void map(Object key, Text value, Context context
                  ) throws IOException, InterruptedException {
    StringTokenizer token = new StringTokenizer(value.toString(), "\t");
    while (token.hasMoreTokens()) {

      //get the value of the second column and count
      String[] wordsplit = value.toString().split("\t");
      wordvalue.set(wordsplit[1]);
      context.write(wordvalue, positive);
    }
  }
}

public static class countReducer
     extends Reducer<Text,IntWritable,Text,IntWritable> {
  //define variables result     
  private IntWritable result = new IntWritable();

  public void reduce(Text key, Iterable<IntWritable> values,
                     Context context
                     ) throws IOException, InterruptedException {
    int sum = 0;
    int number_get;
    for (IntWritable val : values) {
      number_get = val.get();
      sum += number_get; 
    }
    result.set(sum);
    context.write(key, result);
  }
}

  public static void main(String[] args) throws Exception {

    Configuration confd = new Configuration();
    //difference of a node's in and out degree
    Job jobdifference = Job.getInstance(confd, "Q4difference");
    jobdifference.setJarByClass(Q4.class);
    jobdifference.setMapperClass(differenceMapper.class);
    jobdifference.setCombinerClass(differenceReducer.class);
    jobdifference.setReducerClass(differenceReducer.class);
    jobdifference .setOutputKeyClass(Text.class);
    jobdifference.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(jobdifference, new Path(args[0]));
    FileOutputFormat.setOutputPath(jobdifference, new Path(args[1]));
    
    jobdifference.waitForCompletion(true);

    Configuration confc = new Configuration();
    //count nodes with same with same difference
    Job jobcount = Job.getInstance(confc, "Q4count");
    jobcount.setJarByClass(Q4.class);
    jobcount.setMapperClass(countMapper.class);
    jobcount.setCombinerClass(countReducer.class);
    jobcount.setReducerClass(countReducer.class);
    jobcount.setOutputKeyClass(Text.class);
    jobcount.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(jobcount, new Path(args[1]));
    FileOutputFormat.setOutputPath(jobcount, new Path(args[1]));

    System.exit(jobcount.waitForCompletion(true) ? 0 : 1);
  }
}
