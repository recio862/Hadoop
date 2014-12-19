package com.refactorlabs.cs378.sessions;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Renato John Recio
 * 
 * Mapper for UserSessions. Extends class Mapper, provided by Hadoop.
 * Mapper will take User sessions and output session objects
 * as values and the user id and apikey as the key. 
 */
public class SecondMapClass extends Mapper<LongWritable, Text, Text, Text> {


	//Our key (vdpId) and value (userId)
	private Text vdpId = new Text();
	private Text userId = new Text();

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		
		//Split the input by tabs
		String values[] = value.toString().split("\t");
		
		//If we have 2 values, we have the correct input
		if (values.length == 2){
			vdpId.set(values[0]);
			userId.set("UID:"+ values[1]);
			
			//Output format: vdp, userid
			context.write(vdpId, userId);	
			
		
			}

			
	


	}




}
