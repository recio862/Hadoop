package com.refactorlabs.cs378;

import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.refactorlabs.cs378.CustomFormat.COUNTERS;

/**
 * The Map class for word statistics.  Extends class Mapper, provided by Hadoop.
 * This class defines the map() function for the word statistics
 * 
 * @author Renato John Recio (recio862@utexas.edu)
 * @author David Franke (dfranke@cs.utexas.edu)
 */

public class MapClass extends Mapper<Text, NullWritable, Text, WordStatisticsWritable> {

	//Instantiate Text object that will be used for writing to context
	private Text word = new Text();
	
	

	@Override
	public void map(Text key, NullWritable value, Context context)
			throws IOException, InterruptedException {
		long message_length = 0;
		//Convert our text to string
		String line = key.toString();

		//Create tokenizer for input
		StringTokenizer tokenizer = new StringTokenizer(line);

		//Instantiate a map that will tally up the frequencies of each word
		Map<String, Long> wordMap = new TreeMap<String, Long>();


		// For each tokenized word in the input line, 
		// store the word in a map with its new frequency
		while (tokenizer.hasMoreTokens()) {
			message_length++;
			
			String token = tokenizer.nextToken();

			if (!wordMap.containsKey(token)) //Create new k-v pairing
				wordMap.put(token, (long)1);
			else
				wordMap.put(token, wordMap.get(token) + 1); //Use existing k-v pairing 

		}

		
		//Iterate each key in the map (key = word)
		for (String keys : wordMap.keySet()) {
			word.set(keys);

			//Create WordStatisticsWritable object to save our values from wordmap
			WordStatisticsWritable output = new WordStatisticsWritable();
			output.setLongs(1, wordMap.get(keys), wordMap.get(keys) * wordMap.get(keys));
			output.setDoubles(0, 0);
		
			//Write to context
			context.write( word, output);
		}
	
		
		word.set("Message Length");

		//Create WordStatisticsWritable object to save our values from wordmap
		WordStatisticsWritable output = new WordStatisticsWritable();
		output.setLongs(1, message_length, message_length * message_length);
		output.setDoubles(0, 0);
	
		//Write to context
		context.write( word, output);
		
	}

}