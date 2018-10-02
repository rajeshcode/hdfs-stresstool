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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.Iterator;

import java.lang.Thread;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.FsPermission;

import org.apache.commons.collections.buffer.CircularFifoBuffer;

import java.lang.IllegalArgumentException;

@SuppressWarnings("deprecation")
public class StressTool {


  public class StatisticalCircularBuffer {
    private CircularFifoBuffer buf;

    public StatisticalCircularBuffer(int size) {
      this.buf = new CircularFifoBuffer(size);
    }

    public void add(double metric) {
      this.buf.add(new Double(metric));
    }

    public int size() {
      return this.buf.size();
    }

    public double getAvg() {
      Iterator it = buf.iterator();
      double computed_average = 0;
      while(it.hasNext()) {
        Double num = (Double)it.next();
        computed_average += num.doubleValue();
      }
      return computed_average / buf.size();
    }

    public double getStdDev() {
      Iterator it = buf.iterator();
      double computed_std_dev = 0;
      while(it.hasNext()) {
        Double num = (Double)it.next();
        double diff = num.doubleValue() - this.getAvg();
        computed_std_dev += diff*diff;
      }
      return java.lang.Math.sqrt(computed_std_dev / buf.size());
    }
  }


  public class ReportingThread extends Thread {
    private long operations = 0;
    public volatile boolean running = true;
    private long start_time;
    private StatisticalCircularBuffer history;
    private int transients_left = 5; // How many initial points to discard

    private final int HISTORY_LENGTH = 1000;

    public ReportingThread() {
      this.start_time = System.currentTimeMillis();
      this.history = new StatisticalCircularBuffer(HISTORY_LENGTH);
    }

    public void run() {
      while(running) {
        long tdiff = System.currentTimeMillis() - this.start_time;
        double ops_per_second = (double)this.getOperations() / tdiff * 1000;

        if (--transients_left < 0) {
          // Register history when we're at steady state
          history.add(ops_per_second);
          System.out.format("Raw: %f ops/s [avg=%f] [stddev=%f] [size=%d]\n", ops_per_second, history.getAvg(), history.getStdDev(), history.size());
        } else {
          System.out.format("Transients left: %d\n", transients_left);
        }

        // Clear for next run
        this.start_time = System.currentTimeMillis();
        this.clearOperations();

        // Exit if needed
        if (history.size() >= HISTORY_LENGTH) {
          this.running = false;
          System.out.println("Complete.");
          return;
        }

        try{
          Thread.sleep(100);
        } catch(InterruptedException e) {
          e.printStackTrace();
        }
      }
    }

    private synchronized long getOperations() {
      return this.operations;
    }

    public synchronized void register_operation(){
      this.operations = this.operations + 1;
    }

    public synchronized void clearOperations() {
      this.operations = 0;
    }
  }


  public class StressThread extends Thread {
    private FileSystem hdfs;
    public int thread_id;
    private ReportingThread reporter;
    private String operation;

    public StressThread(int thread_id, ReportingThread reporter, String operation) {
      // super();
      setDaemon(true);
      this.thread_id = thread_id;
      this.reporter = reporter;
      this.operation = operation;

      if(!( operation.equals("mkdirs") || operation.equals("chmods") || operation.equals("ls"))) {
        throw new IllegalArgumentException("Operation must be mkdirs or chmods or ls");
      }

      try {
        this.hdfs = FileSystem.get(new org.apache.hadoop.conf.Configuration());
      } catch(java.io.IOException e) {
        e.printStackTrace();
      }
    }

    public String getRandomFilename() {
      return UUID.randomUUID().toString();
    }

    public void log(String message) {
      System.out.format("[%d] %s\n", this.thread_id, message);
    }

    private void doOperation() throws IOException {
      if(this.operation.equals("mkdirs")) {
        String filename = String.format("smallfiles/%s", this.getRandomFilename());
        this.hdfs.mkdirs( new Path(filename) );
      } else if(this.operation.equals("chmods")) {
        Path path = new Path("smallfiles");
        FsPermission perm = FsPermission.getFileDefault();
        this.hdfs.setPermission(path, perm);
      } else if (this.operation.equals("ls")) {
        Path path = new Path("/");
        try {
          this.hdfs.exists(path);
        } catch (Exception e) {
          ; // okay
        }
      }
    }

    public void run() {
      while(reporter.running) {
        try {
          this.doOperation();

          // Report our success
          reporter.register_operation();
        } catch(java.io.IOException e) {
          e.printStackTrace();
        }

        try{
          Thread.sleep(0);
        } catch(InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }

  public void run_stress(String[] args) throws Exception {
    if(args.length != 4) {
      System.err.println("Usage: stresstool stress <mkdirs, chmods, ls> <threads=n> <time in s or -1 for forever>");
      System.err.println("ie: stresstool stress mkdirs 100");
      System.exit(-1);
    }

    // The reporting thread provides metrics
    final ReportingThread reportingThread = new ReportingThread();
    reportingThread.start();

    // Stress threads do the work
    final List<StressThread> threads = new ArrayList<StressThread>();

    String operation = args[1];

    // Start this many threads
    for(int i=0; i < Integer.parseInt(args[2]); i++) {
      StressThread t = new StressThread(i, reportingThread, operation);
      t.start();
      threads.add(t);
    }

    final long secondsToRun = Long.parseLong(args[3]);
    if (secondsToRun > 0) {
      (new Thread() {
        public void run() {
          try {
            Thread.sleep(secondsToRun*1000);
          } catch (InterruptedException ie) {
            ;
          }
          reportingThread.running = false;
        }
      }).start();
    }


    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        System.out.println("Shutdown requested");
        reportingThread.running = false;
      }
   });


    // Run the stress threads forever
    for(int i=0; i < threads.size(); i++) {
      Thread t = threads.get(i);
      t.join(100000);
    }

    reportingThread.join(100000);
  }

  public void run_latency(String[] args) throws Exception {
    long startTime = 0;
    long endTime = 0;
    final int HISTORY_LENGTH = 1000;

    int transients_left = 100;

    FileSystem hdfs = FileSystem.get(new org.apache.hadoop.conf.Configuration());

    // Keep track of a moving average
    StatisticalCircularBuffer history = new StatisticalCircularBuffer(HISTORY_LENGTH);

    while(true) {
      startTime = System.nanoTime();
      hdfs.exists( new Path("smallfiles") );
      endTime = System.nanoTime();

      double nanoseconds = (double)(endTime - startTime);
      history.add(nanoseconds);

      System.out.printf("%d Latency = %f us; avg = %f us; std_dev = %f; size = %d;\n", System.nanoTime(), nanoseconds / 1000, history.getAvg() / 1000, history.getStdDev() / 1000, history.size());

      try {
        Thread.sleep(1000);
      } catch(InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 1) {
      System.err.println("Usage: stresstool <latency, stress>");
      System.exit(2);
    }

    FileSystem hdfs;
    hdfs = FileSystem.get(new org.apache.hadoop.conf.Configuration());
    StressTool st = new StressTool();

    if(otherArgs[0].equals("stress")) {
      // We should do some setup
      st.run_stress(otherArgs);

    } else {
      st.run_latency(otherArgs);
    }

  }
}
