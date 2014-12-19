package com.refactorlabs.cs378;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

//Custom input format class
//Created by: Renato John Recio
//This class is used to create a custom input format using the hadoop inputformat 
//class
public class CustomInputFormat extends InputFormat<Text, NullWritable>{

	//Constant used as identifier  for number of map tasks
	public static final String NUM_MAP_TASKS = "random.generator.map.tasks";
	
	
	//Our custom record reader is initialized and returned in this method
	@Override
	public RecordReader<Text, NullWritable> createRecordReader(InputSplit arg0,
			TaskAttemptContext arg1) throws IOException, InterruptedException {
		RecordReader rr = new CustomRecordReader();
		rr.initialize(arg0, arg1);
		return rr;
	}

	//Gets splits based on how many map tasks we have assigned
	@Override
	public List<InputSplit> getSplits(JobContext arg0) throws IOException,
			InterruptedException {
		int numSplits = arg0.getConfiguration().getInt(NUM_MAP_TASKS, -1);
		
		ArrayList<InputSplit> splits = new ArrayList<InputSplit>();
		//For each int in Num _ map_ tasks, we create a split (map task)
		for (int i = 0 ; i < numSplits ; ++i){
			splits.add(new CustomInputSplit());
		}
		return splits;
	}

	//set our integer for map tasks
	public static void setNumMapTasks(Job job, int i){
		job.getConfiguration().setInt(NUM_MAP_TASKS, i);
	}	
	
	
	
	
}
