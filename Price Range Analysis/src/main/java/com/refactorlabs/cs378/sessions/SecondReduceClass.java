package com.refactorlabs.cs378.sessions;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The Reduce class for UserSessions.  Extends class Reducer, provided by Hadoop.
 * 
 * @author Renato John Recio
 */
public class SecondReduceClass
extends Reducer<Text, Text,
Text, Text> {

	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {


		//Val will store our price
		String val = "";
		//Newstrings will store all user ids
		ArrayList<String> newStrings = new ArrayList<String>();
		//Newstring will store a single userid
		String newString = "";
		for (Text value : values) {
			if (!value.toString().contains("UID:"))
				val = value.toString();
			else{
				//New string generated from value by removing "UID:"
				newString = (value.toString().substring(4, value.toString().length()));
				//Add single userid to list of userids (newStrings)
				newStrings.add(newString);
			}
		}	
		//If we haven't found a price, we are done here
		if (val.equals(""))
			return;
		//For all UID's we have found, output to context using <UID, Price>
		for (int i = 0 ; i < newStrings.size();i++){
			context.write(new Text(newStrings.get(i)), new Text(val));
		}



	}




}





