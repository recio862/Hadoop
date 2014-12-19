package com.refactorlabs.cs378;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * The Map class for Inverted Index.  Extends class Mapper, provided by Hadoop.
 * This class defines the map() function for the inverted index
 * 
 * @author Renato John Recio (recio862@utexas.edu)
 */

public class MapClass extends Mapper<LongWritable, Text, Text, Text> {

	//Instantiate msg variable that will be used for writing our messageId to context
	private Text msg = new Text();

	//Email tags used to determine if we can begin parsing emails for storage
	private final String[] emailTags = {"Message-ID:", "From:", "To:", "Cc:", "Bcc:"};

	//Halt tags to indicate when to stop parsing emails for emailtags
	private final String[] haltTags = {"Subject:" , "Mime-Version:", "X-From:"};


	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException
			{

		//Convert our text to string
		String line = value.toString();

		//Create tokenizer for input
		StringTokenizer tokenizer = new StringTokenizer(line);

		//Initialize lists for various email fields (from, to, cc, bcc)
		List<String> fromList = new ArrayList<String>();
		List<String> toList = new ArrayList<String>();
		List<String> ccList = new ArrayList<String>();
		List<String> bccList = new ArrayList<String>();

		// Iterate through tokenizer and check against email tags
		// and/or halt tags to determine which list to store emails
		while (tokenizer.hasMoreTokens()) {
			String token = tokenizer.nextToken();

			//Step 1: Determine if we have found our message
			//If so, store it in our global variable "msg"
			if (token.equals(emailTags[0])){
				if (tokenizer.hasMoreTokens()){
					String next = tokenizer.nextToken();
					msg.set(next);
				}
			}

			//Step 2: Determine if we have found our from field
			//If so, store it in fromList
			//Note: Only 1 email expected, but we will use this
			//so we can follow the design implementation of the
			//other lists (i.e. we want to strip commas and write
			//to context)
			if (token.equals(emailTags[1])){
				if (tokenizer.hasMoreTokens()){
					String next = tokenizer.nextToken();
					fromList.add("From:" + next);
				}
			}

			//Step 3: Determine if we have our "to" variable. 
			//If so, continue adding tokens to list until we reach
			//the halt tag
			if (token.equals(emailTags[2])){
				while (tokenizer.hasMoreTokens()){
					String next = tokenizer.nextToken();
					if (next.equalsIgnoreCase(haltTags[0])){
						break;
					}
					else
						toList.add("To:" + next);
				}
			}

			//Step 4: Determine if we have our optional "cc" variable. 
			//If so, continue adding tokens to list until we reach
			//the halt tag
			if (token.equals(emailTags[3])){
				while (tokenizer.hasMoreTokens()){
					String next = tokenizer.nextToken();
					if (next.equalsIgnoreCase(haltTags[1]))
						break;
					else
						ccList.add("Cc:" + next);
				}
			}

			//Step 5: Determine if we have our optional "bcc" variable. 
			//If so, continue adding tokens to list until we reach
			//the halt tag
			if (token.equals(emailTags[4])){
				while (tokenizer.hasMoreTokens()){
					String next = tokenizer.nextToken();
					if (next.equalsIgnoreCase(haltTags[2])){
						token = next;
						break;
					}
					else
						bccList.add("Bcc:" + next);
				}
			}

			//Step 6: Halt
			if (token.equals(haltTags[2]))
				break;
		}

		//Strip trailing commas from each email found in the list
		stripCommasFromEmails(fromList);
		stripCommasFromEmails(toList);
		stripCommasFromEmails(ccList);
		stripCommasFromEmails(bccList);

		//WriteListToContext function writes our lists to context
		writeListToContext(fromList, context);
		writeListToContext(toList, context);
		writeListToContext(ccList, context);
		writeListToContext(bccList, context);

			}

	//This function writes a given email list to context
	public void writeListToContext(List<String> emailList, Context context) 	
			throws IOException, InterruptedException{

		//Loop through each string variable and writes as key and uses global msg as value
		for (int i = 0 ; i <emailList.size();i++){
			context.write(new Text(emailList.get(i)), msg);
		}

	}
	
	//Strips trailing commas from the list of emails
	public void stripCommasFromEmails(List<String> list){
		//Loop through the list and parse each email
		for (int i = 0 ; i < list.size() ; i++){
			String currentEmail = list.get(i);
			//If last character is a comma, remove the trailing comma from the current email
			if (currentEmail.charAt(currentEmail.length()-1) == ','){
				currentEmail = currentEmail.substring(0, currentEmail.length()-1);
			list.set(i, currentEmail); //Place the modified email back in the list
			}
		}
		
	}
}