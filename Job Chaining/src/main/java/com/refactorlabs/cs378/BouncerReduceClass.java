package com.refactorlabs.cs378;

import java.io.IOException;

import org.apache.avro.mapreduce.AvroMultipleOutputs;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * The Reduce class for UserSessions.  Extends class Reducer, provided by Hadoop.
 * This class defines the reduce() function for the UserSessions example. Reducer
 * will take bouncer keys and combine their values
 * 
 * @author Renato John Recio
 */
public class BouncerReduceClass
extends Reducer<Text, LongWritable, Text, LongWritable>{


	
	
	
	public void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {

		
		LongWritable finalLongWritable;
		
		//Final long stores our total values
		long finalLong = 0;
		//Iterate through all values
		for (LongWritable value : values) {
			//Store our current value to final long
			finalLong += value.get();
		
	}
		//Create longwritable wrapper
		finalLongWritable = new LongWritable(finalLong);
		//Write to output
		context.write(key, finalLongWritable);
	}

}
