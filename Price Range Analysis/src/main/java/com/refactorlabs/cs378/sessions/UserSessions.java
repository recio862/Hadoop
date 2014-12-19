package com.refactorlabs.cs378.sessions;

import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
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
public class UserSessions extends Configured implements Tool {


	/**
	 * The run() method is called (indirectly) from main(), and contains all the job
	 * setup and configuration.
	 */
	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: UserSessions <input path> <output path>");
			return -1;
		}

		//Get the configuration
		Configuration conf = getConf();
		
		
		//Get the remaining arguments
		String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		//Set the job
		Job job = new Job(conf, "UserSessions");
		Job job2 = new Job(conf, "UserSessions2");
		Job job3 = new Job(conf, "UserSessions3");
		
		// Identify the JAR file to replicate to all machines.
		job.setJarByClass(UserSessions.class);
		job2.setJarByClass(UserSessions.class);
		job3.setJarByClass(UserSessions.class);
		
		// Use this JAR first in the classpath (We also set a bootstrap script in AWS)
		conf.set("mapreduce.user.classpath.first", "true");
		
		// Specify the properties for our first job
		job.setInputFormatClass(AvroKeyInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(FirstMapClass.class);
		job.setReducerClass(FirstReduceClass.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(appArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(appArgs[2]));

		// Specify the properties for our second job
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setMapperClass(SecondMapClass.class);
		job2.setReducerClass(SecondReduceClass.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		MultipleInputs.addInputPath(job2,new Path(appArgs[2] + "/part-r-00000"),TextInputFormat.class, SecondMapClass.class);
		MultipleInputs.addInputPath(job2,new Path(appArgs[1]),TextInputFormat.class,PriceMapClass.class); 
		FileOutputFormat.setOutputPath(job2, new Path(appArgs[2]+"part2"));
		
		// Specify the properties for our third job
		job3.setInputFormatClass(TextInputFormat.class);
		job3.setMapperClass(ThirdMapClass.class);
		job3.setReducerClass(ThirdReduceClass.class);
		job3.setOutputFormatClass(TextOutputFormat.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job3, new Path(appArgs[2]+"part2/part-r-00000"));
		FileOutputFormat.setOutputPath(job3, new Path(appArgs[2]+"part3"));
	
	
		// Initiate the three map-reduce jobs, waiting for completion on each (job chaining)
		job.waitForCompletion(true);
		job2.waitForCompletion(true);
		job3.waitForCompletion(true);
		return 0;
	}

	/**
	 * The main method specifies the characteristics of the map-reduce job
	 * by setting values on the Job object, and then initiates the map-reduce
	 * job and waits for it to complete.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new JobConf(), new UserSessions(), args);
		System.exit(res);
	}

}
