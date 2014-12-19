package com.refactorlabs.cs378;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Renato John Recio
 * 
 * Mapper for UserSessions. Extends class Mapper, provided by Hadoop.
 * Mapper will take User sessions and output session objects
 * as values and the user id and apikey as the key. 
 */
public class BouncerMapClass extends Mapper<AvroKey<Pair<CharSequence, Session>>, NullWritable, Text, LongWritable> {

	//Map for impressions and their count
	private Map<String, Integer> impCount;


	@Override
	public void map(AvroKey<Pair<CharSequence, Session>> key,  NullWritable value, Context context)
			throws IOException, InterruptedException {
		
		//initialize impCount
		impCount = new HashMap<String, Integer>();

		//Grab session
		Session mySession = key.datum().value();

		//Iterate through all impressions
		for (int i = 0 ; i <mySession.getImpressions().size();i++){
			Impression tempImp = mySession.getImpressions().get(i);
			
			//Get impression type
			String impTypeString = getStringFromImpressionType(tempImp.getImpressionType());
			if (impCount.containsKey(impTypeString)) //if it exists, increment
				impCount.put(impTypeString, impCount.get(impTypeString) + 1);
			else //else, put 1
				impCount.put(impTypeString, 1);
		}
		
		//For each impression type, output to context
		for (String impKey : impCount.keySet()){
		context.write(new Text(impKey), new LongWritable((long)impCount.get(impKey)));

		}

	}


	//Get the string translation of the impression type enum
	private String getStringFromImpressionType(ImpressionType impType){

		if (ImpressionType.ACTION == impType)
			return "ACTION";
		else if (ImpressionType.SRP == impType)
			return "SRP";
		else if (ImpressionType.VDP == impType)
			return "VDP";
		else if (ImpressionType.THANK_YOU == impType)
			return "THANK_YOU";
		else
			return "";
	}


}
