package com.refactorlabs.cs378;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * The Double Array Writable class for word statistics.  Extends class 
 * Reducer, provided by Hadoop. This class instantiates a writable
 * double array.
 */
public class WordStatisticsWritable implements Writable{

	private long document_count;
	private long frequency;
	private long frequency_squared;

	
	
	private double mean;
	private double variance;

	
	public WordStatisticsWritable(){
	}

	
	//Sets three longs given as parameters by the mapper / reducer
	public void setLongs(long count, long frequency, long frequency_squared){
		this.document_count = count;
		this.frequency = frequency;
		this.frequency_squared = frequency_squared;
	}

	//Sets two doubles given as parameters by the mapper / reducer
	public void setDoubles(double mean,double variance){
		setMean(mean);
		setVariance(variance);
	}
	

	//Read the values from the DataInput and store them within this class
	@Override
	public void readFields(DataInput arg0) throws IOException {

		//Val stores the values for longs
		long val = arg0.readLong();
		
		setDocument_count(val); //Set doc count
		val = arg0.readLong();
		setFrequency(val); //Set frequency
		val = arg0.readLong();
		setFrequency_squared(val); //Set frequency squared

		//Val2 stores the values for doubles
		double val2 = arg0.readDouble();
		
		setMean(val2); //Set mean
		val2 = arg0.readDouble();
		setVariance(val2); //Set variance
		

	}

	//Write values saved in this class out to DataOutput
	@Override
	public void write(DataOutput arg0) throws IOException {
		//Write document count, frequency, and frequency_squared as longs
		arg0.writeLong(getDocument_count());
		arg0.writeLong(getFrequency());
		arg0.writeLong(getFrequency_squared());

		//Write the mean and variance out as doubles
		arg0.writeDouble(getMean());
		arg0.writeDouble(getVariance());
		

	}

	//toString method concatenates commas and writes out longs and doubles
	@Override
	public String toString(){
		return document_count + "," + frequency + "," +
				frequency_squared+"," + mean + "," + variance;
	}

	
	
	
	
	
	//Getter for doc count
	public long getDocument_count() {
		return document_count;
	}
	
	//Setter for doc count
	public void setDocument_count(long document_count) {
		this.document_count = document_count;
	}
	
	//Getter for frequency
	public long getFrequency() {
		return frequency;
	}
	
	//Setter for frequency
	public void setFrequency(long frequency) {
		this.frequency = frequency;
	}
	
	//Getter for frequency squared
	public long getFrequency_squared() {
		return frequency_squared;
	}
	
	//Setter for frequency squared
	public void setFrequency_squared(long frequency_squared) {
		this.frequency_squared = frequency_squared;
	}
	
	//Getter for mean
	public double getMean() {
		return mean;
	}
	
	//Setter for mean
	public void setMean(double mean) {
		this.mean = mean;
	}
	
	//Getter for variance
	public double getVariance() {
		return variance;
	}
	
	//Setter for variance
	public void setVariance(double variance) {
		this.variance = variance;
	}


	

	
	




}
