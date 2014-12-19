package com.refactorlabs.cs378;

import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * UserSessions main driver program for creating user session objects
 * using Avro
 * 
 * @author Renato John Recio
 * @author David Franke
 *
 *
 */
public class CustomFormat extends Configured implements Tool {
	public static enum COUNTERS {
		  COUNT,
		  LENGTH,
		  LENGTH_SQ
		};

	/**
	 * The run() method is called (indirectly) from main(), and contains all the job
	 * setup and configuration.
	 */
	public int run(String[] args) throws Exception {
		if (args.length < 1) {
			System.err.println("Usage: UserSessions <input path> <output path>");
			return -1;
		}

		//Get the configuration
		Configuration conf = getConf();
		
		
		//Get the remaining arguments
		String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		
		
		//Set the job
		Job job = new Job(conf, "UserSessions");
		
		
		// Identify the JAR file to replicate to all machines.
		job.setJarByClass(CustomFormat.class);
		
		// Use this JAR first in the classpath (We also set a bootstrap script in AWS)
		conf.set("mapreduce.user.classpath.first", "true");

		//Set custom input format
		job.setInputFormatClass(CustomInputFormat.class);
		
		//Set number of map tasks by sending to our custom format
		CustomInputFormat.setNumMapTasks(job, 10);

		
		//Set our classes
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
		job.setCombinerClass(ReduceClass.class);
		
		//Set output key, value, and format
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(WordStatisticsWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		
		//Set output format 
		FileOutputFormat.setOutputPath(job, new Path(appArgs[0]));
	
		job.waitForCompletion(true);
		
		
		Counters counters = job.getCounters();
		double count = 0;
		double freq = 0;
		double freq_sq = 0;
		for (CounterGroup ctrgrp : counters){
			for (Counter ctr: ctrgrp){
				if (ctr.getName().equals("COUNT"))
					count = (double)ctr.getValue();
				if (ctr.getName().equals("LENGTH"))
					freq = (double) ctr.getValue();
				if (ctr.getName().equals("LENGTH_SQ"))
					freq_sq = (double)ctr.getValue();
			}
		}
		
		double mean = ReduceClass.getMean(count, freq);
		double variance = ReduceClass.getVariance(count, mean, freq_sq);
		System.out.println("Mean: " + mean + ", Variance: "+ variance);
		
		//Submit our bouncer and searcher jobs after our initial job
		return 0;
	}

	/**
	 * The main method specifies the characteristics of the map-reduce job
	 * by setting values on the Job object, and then initiates the map-reduce
	 * job and waits for it to complete.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new JobConf(), new CustomFormat(), args);
		System.exit(res);
	}

}
