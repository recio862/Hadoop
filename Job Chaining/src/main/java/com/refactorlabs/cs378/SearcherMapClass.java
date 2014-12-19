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
 * Mapper will take User sessions and output counts for VDP and SRP. 
 */
public class SearcherMapClass extends Mapper<AvroKey<Pair<CharSequence, Session>>, NullWritable, Text, Text> {

	

	//Count of VDP impressions
	private int vdpCount;

	//Count of IDs in SRP impressions
	private int srpCount;


	@Override
	public void map(AvroKey<Pair<CharSequence, Session>> key,  NullWritable value, Context context)
			throws IOException, InterruptedException {

		//Initialize our counts to 0
		vdpCount = 0;
		srpCount = 0;
		
		//Grab our session from the key
		Session mySession = key.datum().value();

		
		//Iterate through impressions
		for (int i = 0 ; i <mySession.getImpressions().size();i++){
			Impression tempImp = mySession.getImpressions().get(i);
			
			//Get our imp type
			String impTypeString = getStringFromImpressionType(tempImp.getImpressionType());
			
			//If we have an impression, increment VDP count
			if (impTypeString.equals("VDP"))
					vdpCount++;
			//If we have an SRP, increment SRP by ID size
			else if (impTypeString.equals("SRP"))
			{
				srpCount+= tempImp.getId().size();
				
			}
				
		}
		
		//Write vdp count and srp count to output
		context.write(new Text("counts"), new Text(vdpCount+","+srpCount));
		

		

	}


	//Given an impression type, return the string translation of the type
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
