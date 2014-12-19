package com.refactorlabs.cs378.sessions;

import java.io.IOException;
import java.util.List;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Renato John Recio
 * 
 * Mapper for UserSessions. Extends class Mapper, provided by Hadoop.
 */
public class FirstMapClass extends Mapper<AvroKey<Pair<CharSequence, Session>>, NullWritable, Text, Text> {


	//Our key (vdpId) and value (userId)
	private Text vdpId = new Text();
	private Text userId = new Text();

	@Override
	public void map(AvroKey<Pair<CharSequence, Session>> key, NullWritable value, Context context)
			throws IOException, InterruptedException {

		

		//Create our session from avro binary (key)
		Session session = key.datum().value();

		//Grab user id from session
		String sessionUserId = session.getUserId().toString();
		
			//set user id from sessionUserId
			userId.set(sessionUserId);
			
			//Create imp list from session's impressions
			List<Impression> impList = session.getImpressions();
			for (int i = 0 ; i < impList.size() ; i++){
				
				//Filter by VDP
				if (impList.get(i).getImpressionType().equals(ImpressionType.VDP)){
					if (!impList.get(i).getId().isEmpty()){
						//Set our key (vdp)
					vdpId.set(""+impList.get(i).getId().get(0));
					//Write to context 
					context.write(vdpId, userId);
					}
				}
				
			}

			
	


	}




}
