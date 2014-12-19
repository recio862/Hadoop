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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * The Reduce class for UserSessions.  Extends class Reducer, provided by Hadoop.
 * This class defines the reduce() function for the UserSessions example. Reducer
 * will take sessions with the same key and combine their leads and impressions into a 
 * single session for output.
 * 
 * @author Renato John Recio
 */
public class ReduceClass
extends Reducer<Text, AvroValue<Session>,
       AvroKey<Pair<CharSequence, Session>>, NullWritable>{


	private AvroMultipleOutputs multipleOutputs;
	
	@Override
	public void reduce(Text key, Iterable<AvroValue<Session>> values, Context context)
			throws IOException, InterruptedException {

		
		
		//finalSession stores our output session
		Session.Builder finalSession = Session.newBuilder();

		//impList will store unsorted impressions
		List<Impression> impList = new ArrayList<Impression>();
		List<Lead> leadList = new ArrayList<Lead>();

		//Flag that keeps track of when we are done storing session info
		boolean flag = true;

		//Iterate through all sessions and take the impressions out to store 
		//into finalSession
		for (AvroValue<Session> value : values) {

			//Save all of our session data using current session object
			//as long as the flag is still true. If we only have leads,
			//flag will remain true. Keep the flag true until we have
			//an impression because impressions have more information
			//that we will need
			if (flag){
				finalSession.setActivex(value.datum().getActivex());
				finalSession.setApiKey(value.datum().getApiKey());
				finalSession.setResolution(value.datum().getResolution());
				finalSession.setUserAgent(value.datum().getUserAgent());
				finalSession.setUserId(value.datum().getUserId());
			}

			//If we have reached an impression, we no longer need to set
			//session values because impressions encompass all necessary
			//session values.
			if (flag && value.datum().getImpressions().size() > 0){
				flag = false;
			}


			//Take the impression data from our session and store in tempImpList
			List<Impression> tempImpList = value.datum().getImpressions();
			for (int i = 0 ; i < tempImpList.size() ; i++){
				//Take each impression and create a copy
				Impression newImpression = Impression.newBuilder(tempImpList.get(i)).build();
				//Store the copied impression in impList
				impList.add(newImpression);
			}

			//Take the leads from our current session and hard copy into lead list
			List<Lead> tempLeadList = value.datum().getLeads();
			for (int i = 0 ; i < tempLeadList.size() ; i++){
				//Take each impression and create a copy
				Lead newLead = Lead.newBuilder(tempLeadList.get(i)).build();
				//Store the copied impression in impList
				leadList.add(newLead);
			}
		}

		//Create our finalImpList using a sorted list of the impressions in impList
		List<Impression> finalImpList = getSortedImpressionList(impList);

		//Set the VDP_Index for our lead list to complete lead list
		setFinalLeadList(leadList, finalImpList, finalSession);

		
		
		//Store the impressions in our final session
		finalSession.setImpressions(finalImpList);
		finalSession.setLeads(leadList);

		
		//Get the output session type 
		String sessionType = getSessionType(leadList, finalImpList);
		
		//Write to context using text built from toString of session object, 
		//only if the sessionType is not discard per the filter
		
		//multipleOutputs.write( sessionType, key, new Text(finalSession.build().toString()));
		multipleOutputs.write(sessionType, new AvroKey<Pair<CharSequence, Session>>
		(new Pair<CharSequence, Session>(key.toString(), finalSession.build())),
NullWritable.get());
	
	}


	//Sets our final lead list for output
	private void setFinalLeadList(List<Lead> leadList, List<Impression> finalImpList, Session.Builder session) {

		//Determines VDP index
		int current_index = -1;

		//Null or empty lead list should return immediately
		if (leadList != null){
			if (leadList.size() == 0)
				return;
		}
		else
			return;

		//Iterate through all leads in the lead list
		for (int lead_index = 0; lead_index < leadList.size(); lead_index++){

			//Find the VDP index for our lead using the list of impressions
			//i represents the index of our imp list
			for (int i = 0 ; i < finalImpList.size() ; i++){

				//If it is of type VDP, we will grab the index
				if (finalImpList.get(i).getImpressionType().equals(ImpressionType.VDP)){

					//Copy the values from the lead and impressions and compare
					long leadLong = leadList.get(lead_index).getId();
					List<Long> impLongs = new ArrayList<Long>(finalImpList.get(i).getId());
					for (int j = 0; j < impLongs.size(); j++){
						if (impLongs.get(j) == leadLong){ //If longs match, we mark our current index
							current_index = i; //Set the vdp index to current imp list index (i)
							break;
						}

					}

				}
				

			}

			//Set the vdp index in our lead (assuming we only have 1 lead, index of lead is 0)
			leadList.get(lead_index).setVdpIndex(current_index);
		}
	}


	//This method takes an unsorted list of Impressions and sorts them based on their
	//timestamp information
	private List<Impression> getSortedImpressionList(List<Impression> impList) {

		//Create a list of ComparableImpression which wrap
		//Impression objects with comparable interface
		List<ComparableImpression> sortedImpList = new ArrayList<ComparableImpression>();
		for (int i = 0 ; i < impList.size() ; i++){
			ComparableImpression sortedImpression = new ComparableImpression(impList.get(i));
			sortedImpList.add(sortedImpression);
		}

		//Sort impressions by timestamp using ComparableImpression class
		Collections.sort(sortedImpList);

		//Unwrap ComparableImpressions and store in finalImpList
		List<Impression> finalImpList = new ArrayList<Impression>();
		for (int i = 0 ; i < sortedImpList.size() ; i++){
			finalImpList.add(i, sortedImpList.get(i).getMyImp());
		}

		//Return sorted Impression list
		return finalImpList;

	}


	//Setup method to initialzie multiple outputs
	@Override
	public void setup(Context context) {
		multipleOutputs = new AvroMultipleOutputs(context);
	} 

	//Cleanup method to close multiple outputs
	@Override
	public void cleanup(Context context) throws InterruptedException,IOException {
		multipleOutputs.close();
	} 
	
	//Get our session type using the assignment criteria
	private String getSessionType(List<Lead> leadList, List<Impression> impList){
		//If more than 1 lead, it is a submitter
		if (leadList.size() > 0)
			return "submitter";
		//If leads are null and impression size is 1, it is a bouncer
		if (leadList.size() == 0 && impList.size() == 1)
			return "bouncer";
		//If any of the impressions are not SRP, searcher
		for (int i = 0 ; i < impList.size(); i++){
			if (impList.get(i).getImpressionType() != ImpressionType.SRP)
				return "searcher";
			
		}
		//All else , browser
		return "browser";
	}

	

}
