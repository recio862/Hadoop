package com.refactorlabs.cs378;

import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * The Map class for word statistics.  Extends class Mapper, provided by Hadoop.
 * This class defines the map() function for the word statistics
 * 
 * @author Renato John Recio (recio862@utexas.edu)
 * @author David Franke (dfranke@cs.utexas.edu)
 */

public class MapClass extends Mapper<LongWritable, Text, Text, DoubleArrayWritable> {
	
	//Instantiate Text object that will be used for writing to context
	private Text word = new Text();

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		//Convert our text to string
		String line = value.toString();
		
		//Create tokenizer for input
		StringTokenizer tokenizer = new StringTokenizer(line);
		
		//Instantiate a map that will tally up the frequencies of each word
		Map<String, Double> wordMap = new TreeMap<String, Double>();
		
		
		// For each tokenized word in the input line, 
		// store the word in a map with its new frequency
		while (tokenizer.hasMoreTokens()) {
			String token = tokenizer.nextToken();
			
			if (!wordMap.containsKey(token)) //Create new k-v pairing
				wordMap.put(token, 1.);
			else
				wordMap.put(token, wordMap.get(token) + 1); //Use existing k-v pairing
		}

		//Iterate each key in the map (key = word)
		for (String keys : wordMap.keySet()) {
			word.set(keys);
			//Store each value in the writable array
			Writable[] writable = new Writable[3];
			//Writable[0] represents DOCUMENT_COUNT
			writable[0] = new DoubleWritable(1D); 
			//Writable[1] represents FREQUENCY
			writable[1] = new DoubleWritable(wordMap.get(keys));
			//Writable[2] represents FREQUENCY_SQUARED
			writable[2] = new DoubleWritable(wordMap.get(keys) * wordMap.get(keys));
			//Store writable array in a DoubleArrayWritable object
			DoubleArrayWritable dw = new DoubleArrayWritable();
			dw.set(writable);
			//Write to context
			context.write( word, dw);
		}

	}

}