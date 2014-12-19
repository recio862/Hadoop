package com.refactorlabs.cs378;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The Reduce class for UserSessions.  Extends class Reducer, provided by Hadoop.
 * This class defines the reduce() function for the UserSessions example. Reducer
 * will take sessions with the same key and combine their impressions into a 
 * single session for output.
 * 
 * @author Renato John Recio
 */
public class ReduceClass
extends Reducer<Text, AvroValue<Session>,
AvroKey<Pair<CharSequence, Session>>, NullWritable> {


	@Override
	public void reduce(Text key, Iterable<AvroValue<Session>> values, Context context)
			throws IOException, InterruptedException {

		//finalSession stores our output session
		Session.Builder finalSession = Session.newBuilder();

		//impList will store unsorted impressions
		List<Impression> impList = new ArrayList<Impression>();

		//Flag that keeps track of first iteration
		boolean firstIteration = true;

		//Iterate through all sessions and take the impressions out to store 
		//into finalSession
		for (AvroValue<Session> value : values) {

			//Save all of our session data using the first session object
			if (firstIteration){
				finalSession.setActivex(value.datum().getActivex());
				finalSession.setApiKey(value.datum().getApiKey());
				finalSession.setResolution(value.datum().getResolution());
				finalSession.setUserAgent(value.datum().getUserAgent());
				finalSession.setUserId(value.datum().getUserId());
				firstIteration = false;
			}

			//Take the impression data from our session and store in tempImpList
			List<Impression> tempImpList = value.datum().getImpressions();
			for (int i = 0 ; i < tempImpList.size() ; i++){
				//Take each impression and create a copy
				Impression newImpression = Impression.newBuilder(tempImpList.get(i)).build();
				//Store the copied impression in impList
				impList.add(newImpression);
			}
		}

		//Create our finalImpList using a sorted list of the impressions in impList
		List<Impression> finalImpList = getSortedImpressionList(impList);

		//Store the impressions in our final session
		finalSession.setImpressions(finalImpList);

		//Write to context using avroKey with key and final session
		context.write(
				new AvroKey<Pair<CharSequence, Session>>
				(new Pair<CharSequence, Session>(key.toString(), finalSession.build())),
				NullWritable.get());
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
}

