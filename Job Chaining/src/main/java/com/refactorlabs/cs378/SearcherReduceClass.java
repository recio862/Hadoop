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
 * will take sessions with the same key and combine their VDP and SRP and then
 * compute the click thru rate.
 * 
 * @author Renato John Recio
 */
public class SearcherReduceClass
extends Reducer<Text, Text, Text, NullWritable>{


	
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		
		//Initialize vdp and srp counts and click through rate
		long vdpCount = 0;
		long srpCount = 0;
		double clickThroughRate = 0;
	
		//Iterate through all values
		for (Text t: values){
			String keyToString = t.toString();
			String[] strings = keyToString.split(",");
			if (strings.length ==2){ //Make sure we have valid value
				//If valid, add to our counts
				vdpCount += Long.parseLong(strings[0]);
				srpCount += Long.parseLong(strings[1]);
			}
		}
		
		//Click through rate formula = vdp/srpids
		clickThroughRate = (double)((double)vdpCount / (double)srpCount);
		
		//Create a text output object for our final text
		Text finalText = new Text(vdpCount + "," + srpCount + "," + clickThroughRate);
		
		//Write to context
		context.write(finalText, NullWritable.get());
	}

}
