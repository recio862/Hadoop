package com.refactorlabs.cs378;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

/**
 * The Double Array Writable class for word statistics.  Extends class 
 * Reducer, provided by Hadoop. This class instantiates a writable
 * double array.
 */
public class DoubleArrayWritable extends ArrayWritable{

	//Instantiate DoubleArrayWritable using DoubleWritable.class
	public DoubleArrayWritable() {
		super(DoubleWritable.class);
	} 
	
	//Return the values held within the DoubleArrayWritable class
	public double[] getValueArray(){
		Writable[] writableValues = get();
		double[] valueArray = new double[writableValues.length];
		for (int i = 0 ; i < valueArray.length; i++){
			valueArray[i] = ((DoubleWritable)(writableValues[i])).get();
		}
		return valueArray;
	}
	
	
	
	
}
