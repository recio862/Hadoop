package com.refactorlabs.cs378;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * The Reduce class for WordStatistics and WordStatisticsAggregator.
 * Extends class Reducer, provided by Hadoop.
 * @author Renato John Recio (recio862@utexas.edu)
 * @author David Franke (dfranke@cs.utexas.edu)
 */

public class ReduceClass extends Reducer<Text, WordStatisticsWritable, Text, WordStatisticsWritable> {

	//Returns the variance from the document_count, mean, and frequency_squared parameters
	//Calculates using the formula E[x^2] - (E[x])^2
	public static double getVariance(double document_count, double mean, double frequency_squared){
		double expected_value_1 = ((frequency_squared) / (document_count));
		double expected_value_2 = (mean * mean);
		double variance = expected_value_1 - expected_value_2;

		return variance;
	}

	//Returns the mean from the document_count and frequency parametes
	//Calculates using the formula frequency / number of documents
	public static double getMean(double document_count, double frequency){
		double mean = frequency/document_count;
		return mean;

	}

	@Override
	public void reduce(Text key, Iterable<WordStatisticsWritable> values, Context context)
			throws IOException, InterruptedException {

		//Instantiate message variables that will store our output values

		//Instantiate variables that will store our output values
		long document_count = 0, frequency = 0, frequency_squared = 0;

		// Sum up the counts for the current word, specified in object "key".
		for (WordStatisticsWritable value : values) {
			//Grab the values in the array

			//Sum up the values and store in respective variables
			document_count += value.getDocument_count();
			frequency += value.getFrequency();
			frequency_squared += value.getFrequency_squared();
		}

		//Get the mean and variance for our word
		double mean = getMean(document_count, frequency);
		double variance = getVariance(document_count, mean, frequency_squared);



		//Create our custom WordStatisticsWritable object
		WordStatisticsWritable output = new WordStatisticsWritable();

		//Store the longs in WordStatisticsWritable
		output.setLongs(document_count, frequency, frequency_squared);
		//Store the doubles in WordStatisticsWritable
		output.setDoubles(mean, variance);
	
		
		//Write the output to context
			context.write(key, output);
	}

	
}