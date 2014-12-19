package com.refactorlabs.cs378;

import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.avro.mapred.Pair;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;

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
		if (args.length < 3) {
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

		// Specify the Map
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(ImpressionMapClass.class);
		job.setMapOutputKeyClass(Text.class);
		AvroJob.setMapOutputValueSchema(job, Session.getClassSchema());

		//Job2 represents our bouncer map-reduce job
		job2.setInputFormatClass(AvroKeyInputFormat.class);
		job2.setMapperClass(BouncerMapClass.class);
		job2.setReducerClass(BouncerReduceClass.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(LongWritable.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job2, new Path(appArgs[2] + "/bouncer-r-00000.avro"));
		
		
		//Job3 represents our searcher map-reduce job
		job3.setInputFormatClass(AvroKeyInputFormat.class);
		job3.setMapperClass(SearcherMapClass.class);
		job3.setReducerClass(SearcherReduceClass.class);
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);
		job3.setOutputFormatClass(TextOutputFormat.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job3, new Path(appArgs[2] + "/searcher-r-00000.avro"));
		
		
		// Specify the Reduce
		job.setOutputFormatClass(AvroKeyOutputFormat.class);
		job.setReducerClass(ReduceClass.class);
		job.setOutputValueClass(NullWritable.class);
		AvroJob.setOutputKeySchema(job,
				Pair.getPairSchema(Schema.create(Schema.Type.STRING), Session.getClassSchema()));

		//Process command line input and send to appropriate mapper 
		//First input line is impression mapper, second is lead mapper
		MultipleInputs.addInputPath(job,new Path(appArgs[0]),TextInputFormat.class,ImpressionMapClass.class);
		MultipleInputs.addInputPath(job,new Path(appArgs[1]),TextInputFormat.class,LeadMapClass.class); 

		//Add multiple outputs for our 4 types
		AvroMultipleOutputs.addNamedOutput(job, "submitter",AvroKeyOutputFormat.class,Pair.getPairSchema(Schema.create(Schema.Type.STRING), Session.getClassSchema()), null); 
		AvroMultipleOutputs.addNamedOutput(job, "searcher",AvroKeyOutputFormat.class,Pair.getPairSchema(Schema.create(Schema.Type.STRING), Session.getClassSchema()), null); 
		AvroMultipleOutputs.addNamedOutput(job, "bouncer",AvroKeyOutputFormat.class,Pair.getPairSchema(Schema.create(Schema.Type.STRING), Session.getClassSchema()), null); 
		AvroMultipleOutputs.addNamedOutput(job, "browser",AvroKeyOutputFormat.class,Pair.getPairSchema(Schema.create(Schema.Type.STRING), Session.getClassSchema()), null); 
		
		//Set output format 
		FileOutputFormat.setOutputPath(job, new Path(appArgs[2]));
		FileOutputFormat.setOutputPath(job2, new Path(appArgs[2]+"bouncer"));
		FileOutputFormat.setOutputPath(job3, new Path(appArgs[2]+"searcher"));
		
		// Initiate the map-reduce job, and wait for completion.
		job.waitForCompletion(true);
		
		//Submit our bouncer and searcher jobs after our initial job
		job2.submit();
		job3.submit();
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
