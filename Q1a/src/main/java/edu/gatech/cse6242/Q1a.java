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

public class Q1a {

  public static class TokenizerMapper
     extends Mapper<Object, Text, IntWritable, IntWritable>{

    //define variables about node and weight
    private IntWritable node = new IntWritable();
    private IntWritable weight = new IntWritable();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      StringTokenizer token = new StringTokenizer(value.toString(), "\n");
      while (token.hasMoreTokens()) {
        String word = token.nextToken();
        String wordsplit[] = word.split("\t");
        if (wordsplit.length == 3) {
         //give node the tgt value
        node.set(Integer.parseInt(wordsplit[0]));
        //give weight the weight value
        weight.set(Integer.parseInt(wordsplit[2]));
        } else {
          //give node the tgt value
        node.set(Integer.parseInt(wordsplit[0]));
        //give weight the weight value
        weight.set(0);
        }
        
        //write them into context
        context.write(node, weight);
    }
  }
}

public static class IntMaxReducer
     extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
  //define variables result  
  private IntWritable result = new IntWritable();

  public void reduce(IntWritable key, Iterable<IntWritable> values,
                     Context context
                     ) throws IOException, InterruptedException {
    int max = 0;
    int number_get;
    for (IntWritable val : values) {
      number_get = val.get();
      if(number_get >= max) {
          max = number_get;
      }
    }
     result.set(max);
     context.write(key, result);
  }
}


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Q1a");
    job.setJarByClass(Q1a.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntMaxReducer.class);
    job.setReducerClass(IntMaxReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
