package com.refactorlabs.cs378;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class CustomRecordReader extends RecordReader{
	private static final String[] words = { "The", "quick", "brown", "fox", "jumped", "over", "the", "lazy", "dog", "sir"};
	
	private int numRecordsToCreate;
	private int recordsCreated;
	
	//Get random word using Random() 
	private String getRandomWord(){
		Random r = new Random();
		int val = r.nextInt(10);
		//Grab our random from our words
		String word = words[val];
		
		return word;
	}
	
	//Get message composed of random words 
	private String getMessage(){
		String message = "";
		int max = getRandomNumber();
		//Max determined by random number, messages concatenated to message
		for (int i = 0 ; i < max; i++){
			message+= getRandomWord();
			message+= "\n";
		}
		return message;
	}
	
	//Gets random number with std dev 10 and mean 50 using nextGaussian
	private int getRandomNumber(){
		Random r = new Random();
		int val = (int) (r.nextGaussian() * 10 + 50);
		//Error check
		while (val < 1 || val > 99)
			val = (int) (r.nextGaussian() * 10 + 50);
		return val;
	}
	
	@Override
	public void close() throws IOException {
	}

	//Get our message
	@Override
	public Object getCurrentKey() throws IOException, InterruptedException {
		return new Text(getMessage());
	}

	//Return null 
	@Override
	public Object getCurrentValue() throws IOException, InterruptedException {
		return NullWritable.get();
	}

	//Our progress
	@Override
	public float getProgress() throws IOException, InterruptedException {
		return (float)recordsCreated / (float)numRecordsToCreate;
	}

	//Initialize by setting number of records to create & records created
	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException {
		numRecordsToCreate = 100000;
		recordsCreated = 0;
		
	}

	//Return the next key value by decrementing / incrementing our two variables
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		recordsCreated ++;
		numRecordsToCreate --;
		//We have reached our final value
		if (recordsCreated == 100001)
			return false;
		return true;
	}

}
