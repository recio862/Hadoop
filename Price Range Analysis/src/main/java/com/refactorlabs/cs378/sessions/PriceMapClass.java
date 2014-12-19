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
public class PriceMapClass extends Mapper<LongWritable, Text, Text, Text> {


	
	 //Our key (vdpId) and value (price)
	private Text vdpId = new Text();
	private Text price = new Text();

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		
		//Split the input based on ","
		String values[] = value.toString().split(",");
		
		//We need values to be length 2 for proper format
		if (values.length == 2){
		vdpId = new Text(values[0]);
		price = new Text(values[1]);
		//Write our values to context 
		context.write(vdpId, price);	
		}


	}




}
