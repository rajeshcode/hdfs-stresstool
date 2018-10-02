/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

public class StressToolMR {
  public enum MyCounters {
  Runs, Skips
  }

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, IntWritable>{
    
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      // Check to see if time's up

      Configuration conf = context.getConfiguration();
      long startTime = conf.getLong("StressToolStartTime", 0);
      long currentTime = System.currentTimeMillis();

      if ( (startTime + 60*1000) < currentTime) {
        context.getCounter(MyCounters.Skips).increment(1);
        return; // Time expired
      }

      // Now we're gated. Wait for the time to pass before starting
      while ( (startTime + 60*1000) > System.currentTimeMillis() ) {
        Thread.sleep(1000);
      }

      long threads = conf.getLong("Threads", 1);
      StressTool st = new StressTool();
      String[] args = new String[]{"unused", "ls", Long.toString(threads), "60"};
      try {
        st.run_stress(args);
      } catch (Exception e) {
        ;// okay
      }
      context.getCounter(MyCounters.Runs).increment(1);
    }
  }
  
  public static class IntSumReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      ; // Don't reduce
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 3) {
      System.err.println("Usage: stress <in> <out> <threads>");
      System.exit(2);
    }

    conf.setLong("StressToolStartTime", System.currentTimeMillis());
    conf.setLong("Threads", Long.parseLong(otherArgs[2]) );

    Job job = new Job(conf, "StressTool");

    job.setJarByClass(StressToolMR.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setNumReduceTasks(0);
    // job.setCombinerClass(IntSumReducer.class);
    // job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(NullOutputFormat.class);
    job.setOutputValueClass(NullOutputFormat.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
