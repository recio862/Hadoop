package com.refactorlabs.cs378;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The Reduce class for InvertedIndex.  Extends class Reducer, provided by Hadoop.
 * @author Renato John Recio (recio862@utexas.edu)
 */

public class ReduceClass extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {


		//Initially store the values in a set to avoid adding duplicates
		//NOTE: This is for Bonus Part 1
		Set<String> set = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);

		// Store all the text values into our treeSet
		for (Text value : values) {
			String currentVal = value.toString();
			set.add(currentVal);
		}

		//Place the set back into an ArrayList (messageIdList)
		List<String> messageIdList = new ArrayList<String>(set);

		//Sort the messageID values
		//NOTE: This is for Bonus part 2
		Collections.sort(messageIdList);

		
		//Add commas to the list of messages (if x > 0) 
		//Then store into Text variable (msg) for output
		Text msg = addCommasToMessageList(messageIdList);
		
		//Write the output to context
		context.write(key, msg);
	}
	
	//Adds commas in between messages when there is more than 1 message
	public static Text addCommasToMessageList(List<String> messageIdList) {
		String result = "";
		for (int i = 0 ; i < messageIdList.size(); i++){
			if (i == 0 )
				result += messageIdList.get(i); //Append first message to result
			else
				result+= ","+messageIdList.get(i); //Add commas in between messages
		}
		return new Text(result); //Return result as a Text object for output
	}
}