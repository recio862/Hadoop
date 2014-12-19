package com.refactorlabs.cs378.sessions;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The Reduce class for UserSessions.  Extends class Reducer, provided by Hadoop.
 * This class defines the reduce() function for the UserSessions example. Reducer
 * will take sessions with the same key and combine their leads and impressions into a 
 * single session for output.
 * 
 * @author Renato John Recio
 */
public class FirstReduceClass
extends Reducer<Text, Text,
Text, Text> {

	//Our key (vdpId) and value (userId)
	private Text vdpId = new Text();
	private Text userId = new Text();

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		//vdpid will be the key
		vdpId.set(key);
		
		//Iterate all values and write to context 
		for (Text value : values) {
			userId.set(value);
			context.write(vdpId, userId);

		}



	}




}
