package com.refactorlabs.cs378;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * MapReduce program that combines the output of
 * multiple jobs from the WordStatistics program.
 *
 * @author Renato John Recio (recio862@utexas.edu)
 * @author David Franke (dfranke@cs.utexas.edu)
 */
public class WordStatisticsAggregator {

	
	/**
	 * The main method specifies the characteristics of the map-reduce job
	 * by setting values on the Job object, and then initiates the map-reduce
	 * job and waits for it to complete.
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		Job job = new Job(conf, "wordstatistics");
		// Identify the JAR file to replicate to all machines.
		job.setJarByClass(WordStatisticsAggregator.class);

		// Set the output key and value types (for map and reduce).
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(WordStatisticsWritable.class);

		// Set the map and reduce classes.
		job.setMapperClass(AggregatorMapClass.class);
		job.setCombinerClass(ReduceClass.class);
		job.setReducerClass(ReduceClass.class);

		// Set the input and output file formats.
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// Grab the input file and output directory from the command line.
		String[] inputPaths = appArgs[0].split(",");
		for ( String inputPath : inputPaths ) {
		  FileInputFormat.addInputPath(job, new Path(inputPath));
		} 
		
		FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));

		// Initiate the map-reduce job, and wait for completion.
		job.waitForCompletion(true);
	}
}
