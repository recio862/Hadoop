package com.refactorlabs.cs378;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * The Map class for word statistics aggregator.  Extends class Mapper, provided by Hadoop.
 * This class defines the map() function for the word statistics aggregator
 * 
 * @author Renato John Recio (recio862@utexas.edu)
 * @author David Franke (dfranke@cs.utexas.edu)
 */

public class AggregatorMapClass extends Mapper<Text, Text, Text, WordStatisticsWritable> {



	@Override
	public void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {

		//Convert our text to string
		String line = value.toString();

		//Remove commas from line
		line = separateCommas(line);

		//Initialize longs and doubles
		long document_count = 0 , frequency = 0, frequency_squared = 0 ;
		double mean= 0, variance = 0;

		//Create tokenizer for input
		StringTokenizer tokenizer = new StringTokenizer(line);
		
		//Track the index of the current token to determine which value
		//we are currently at
		int value_index = 0;
		
		// For each tokenized word in the input line, 
		// store the word in a map with its new frequency
		while (tokenizer.hasMoreTokens()) {
			
			//Use conditional statements against the value index
			//to determine which value we are currently saving
			
			String token = tokenizer.nextToken();
			
			if ( value_index == 0) 
				document_count = Long.parseLong(token);
			if ( value_index == 1)
				frequency = Long.parseLong(token);
			if ( value_index == 2)
				frequency_squared = Long.parseLong(token);
			if ( value_index == 3)
				mean = Double.parseDouble(token);
			if ( value_index == 4)
				variance = Double.parseDouble(token);


			value_index++;

		}


		//Create our WordStatisticsWritable object
		WordStatisticsWritable output = new WordStatisticsWritable();
		
		//Set the longs and doubles inside of WordStatisticsWritable
		output.setLongs(document_count, frequency, frequency_squared);
		output.setDoubles(mean, variance);
		
		//Write to context
		context.write( key , output);
	}

	
	//Removes commas from the line and replace with spaces for the tokenizer
	private String separateCommas(String line) {
		String result = "";
		for (int i = 0 ; i < line.length(); i++){
			if (line.charAt(i) != ',')
				result += line.charAt(i);
			else
				result += " ";
		}
		return result;
	}

}

