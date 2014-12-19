package com.refactorlabs.cs378;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The Combiner class for word statistics.  Extends class Reducer, provided by Hadoop.
 * @author Renato John Recio (recio862@utexas.edu)
 * @author David Franke (dfranke@cs.utexas.edu)
 */
public class CombineClass extends Reducer<Text, DoubleArrayWritable, Text, DoubleArrayWritable> {


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
		
		//Store all the output values into a DoubleArrayWritable object 
		Writable[] writable = new Writable[3];
		writable[0] = new DoubleWritable(document_count);
		writable[1] = new DoubleWritable(frequency);
		writable[2] = new DoubleWritable(frequency_squared);
		DoubleArrayWritable dw = new DoubleArrayWritable();
		dw.set(writable);
		
		//Write the key and DoubleArrayWritable to context
		context.write(key, dw);
	}
}