package com.refactorlabs.cs378.sessions;

import java.io.IOException;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
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
public class ThirdReduceClass
extends Reducer<Text, Text,
Text, Text> {



	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		//Initialize our statistics generator
		DescriptiveStatistics s = new DescriptiveStatistics();
		
		//Price count
		double priceCount = 0.0;
		
		
		//Each value will be a price from our key (userid)
		for (Text value : values) {
			priceCount++; //increment price count for each list item
			
			//parse our value
			double val = Double.parseDouble(value.toString());
			
			//Add value to statistics
			s.addValue(val);
			}	
	
		//Generate our statistics string
		String statistics = "";
		statistics+= (""+priceCount +","+s.getMin()+","+s.getMax()+","+s.getMean()+","+s.getPercentile(50) + "," + s.getStandardDeviation()+"," + s.getSkewness() +"," + s.getKurtosis());
				
		//Write userid and statistics to output
		context.write(key, new Text(statistics));		
			
	}

		
		
		

	




}
