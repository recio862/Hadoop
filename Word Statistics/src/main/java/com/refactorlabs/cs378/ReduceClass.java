package com.refactorlabs.cs378;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The Reduce class for word statistics.  Extends class Reducer, provided by Hadoop.
 * @author Renato John Recio (recio862@utexas.edu)
 * @author David Franke (dfranke@cs.utexas.edu)
 */
 
public class ReduceClass extends Reducer<Text, DoubleArrayWritable, Text, Text> {

	//Returns the variance from the document_count, mean, and frequency_squared parameters
	//Calculates using the formula E[x^2] - (E[x])^2
	public double getVariance(double document_count, double mean, double frequency_squared){
		double expected_value_1 = ((frequency_squared) / (document_count));
		double expected_value_2 = (mean * mean);
		double variance = expected_value_1 - expected_value_2;
		
		return variance;
	}
	
	//Returns the mean from the document_count and frequency parametes
	//Calculates using the formula frequency / number of documents
	public double getMean(double document_count, double frequency){
		double mean = frequency/document_count;
		return mean;
		
	}
	
	@Override
	public void reduce(Text key, Iterable<DoubleArrayWritable> values, Context context)
			throws IOException, InterruptedException {
		
		//Instantiate variables that will store our output values
		double document_count = 0, frequency = 0, frequency_squared = 0;
		
		// Sum up the counts for the current word, specified in object "key".
		for (DoubleArrayWritable value : values) {
			//Grab the values in the array
			double[] w = value.getValueArray();
			//Sum up the values and store in respective variables
			document_count += w[0];
			frequency += w[1];
			frequency_squared += w[2];
		
		}
		
		//Get the mean and variance for our word
		double mean = getMean(document_count, frequency);
		double variance = getVariance(document_count, mean, frequency_squared);
		
		//Create a text object for CSV output
		Text tx = new Text();
		String output = document_count + "," + mean + "," + variance;
		tx.set(output);
		
		
		//Write the output to context
		context.write(key, tx);
	}
}