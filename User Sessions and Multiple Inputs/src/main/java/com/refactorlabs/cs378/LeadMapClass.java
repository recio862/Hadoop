package com.refactorlabs.cs378;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Renato John Recio
 * 
 * Mapper for UserSessions. Extends class Mapper, provided by Hadoop.
 * Mapper will take User sessions and output session objects
 * as values and the user id and apikey as the key. 
 */
public class LeadMapClass extends Mapper<LongWritable, Text, Text, AvroValue<Session>> {


	/**
	 * Local variable "word" will contain the key for our output
	 * The key is simply a concatenation of the apikey and userid
	 * 	 
	 * */
	private Text word = new Text();
	private String line;

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		//Convert our text to string
		line = value.toString();

		//Create our session and impression builders
		Session.Builder sessionBuilder = Session.newBuilder();
		Lead.Builder leadBuilder = Lead.newBuilder();

		//////////////////////////////////////////////////////////
		//Initialize all of our avro fields
		String idnumbers = getFieldValue("|id:");
		idnumbers = idnumbers.replaceAll(",", " ");
		String resolution = getFieldValue("res:");
		String uagent = getFieldValue("uagent:");
		String apikey = getFieldValue("apikey:");
		String uid= getFieldValue("|userid:");
		String leadid = getFieldValue("lead_id:");
		String type = getFieldValue("|type:");
		String bidtype = getFieldValue("|bidtype:");
		String advertiser = getFieldValue("advertiser:");
		String campaignid = getFieldValue("campaign_id:");
		String idnew = getFieldValue("|recordid:");
		String amount = getFieldValue("lead_amount:");
		String revenue = getFieldValue("revenue:");
		String test = getFieldValue("test:");
		String ab = getFieldValue("ab:");
		//////////////////////////////////////////////////////////

		//////////////////////////////////////////////////////////
		//Set all of our non-enumerated session field values
		sessionBuilder.setApiKey(getValidatedFieldValue(apikey));
		sessionBuilder.setUserId(getValidatedFieldValue(uid));
		sessionBuilder.setResolution(getValidatedFieldValue(resolution));
		sessionBuilder.setUserAgent(getValidatedFieldValue(uagent));
		//////////////////////////////////////////////////////////


		//////////////////////////////////////////////////////////
		//Set all of our non-enumerated lead field values
		leadBuilder.setAb(ab);
		leadBuilder.setRevenue(parseValidFloat(revenue));
		leadBuilder.setAmount(parseValidFloat(amount));
		leadBuilder.setId(parseValidLong(idnew));
		leadBuilder.setCampaignId(campaignid);
		leadBuilder.setAdvertiser(advertiser);
		leadBuilder.setLeadId(parseValidLong(leadid));
		leadBuilder.setVdpIndex(-1); //Temporary default

		//////////////////////////////////////////////////////////


		//////////////////////////////////////////////////////////
		//Set all of our session field values that are enumerated
		//Simply check to see if string exists in the input text
		//And enumeration based on string
		if (line.contains("activex:enabled"))
			sessionBuilder.setActivex(ActiveX.ENABLED);
		else
			sessionBuilder.setActivex(ActiveX.NOT_SUPPORTED);
		//////////////////////////////////////////////////////////

		//////////////////////////////////////////////////////////
		//Set all of our lead field values that are enumerated
		//Simply check to see if string exists in the input text
		//And set enumerations based on string
		if (type.equals("good"))
			leadBuilder.setType(LeadType.GOOD);
		else if (type.equals("bad"))
			leadBuilder.setType(LeadType.BAD);
		else if (type.equals("range"))
			leadBuilder.setType(LeadType.RANGE);
		else if (type.equals("error"))
			leadBuilder.setType(LeadType.ERROR);
		else if (type.equals("duplicate"))
			leadBuilder.setType(LeadType.DUPLICATE);
		else
			leadBuilder.setType(LeadType.BAD);

		if (bidtype.equals("lead"))
			leadBuilder.setBidType(BidType.LEAD);
		else if (bidtype.equals("sale"))
			leadBuilder.setBidType(BidType.SALE);
		else if (bidtype.equals("other"))
			leadBuilder.setBidType(BidType.OTHER);


		if (test.equals("true"))
			leadBuilder.setTest(true);
		else
			leadBuilder.setTest(false);
		//////////////////////////////////////////////////////////


		//Set our output key using concatenated uid and apikey
		word.set(uid+":"+apikey);


		//Create lead list out of our input and store in the session
		List<Lead> leadList = new ArrayList<Lead>();
		leadList.add(leadBuilder.build());
		sessionBuilder.setLeads(leadList);

		//Set empty impression list in the session
		List<Impression> impList = new ArrayList<Impression>();
		sessionBuilder.setImpressions(impList);

		//Write key and session (wrapped in avro value) to context
		context.write( word, new AvroValue<Session>(sessionBuilder.build()));


	}



	//Parse valid long and return 0 if invalid
	private Long parseValidLong(String key){
		if (getValidatedFieldValue(key) != null)
			return Long.parseLong(key);
		else
			return (long)0;
	}


	//Parse valid integer and return 0 if invalid
	private Float parseValidFloat(String key) {
		if (getValidatedFieldValue(key) != null)
			return Float.parseFloat(key);
		else
			return (float) 0.0;
	}



	//Validates a field value and determines if
	private String getValidatedFieldValue(String fieldValue){
		//If the length of the field value is 0, it must be invalid
		//If the field value contains the object object value, it should be null
		if (fieldValue.length() > 0 && !(fieldValue.toUpperCase().contains("[OBJECT OBJECT]")))
			return fieldValue;
		else
			return null;
	}

	//Get the value of the field passed in as the fieldKey
	//Uses the line (input) to find the value
	private String getFieldValue(String fieldKey){
		String result = "";

		//Find position occurrence of the fieldKey
		int index = line.indexOf(fieldKey);

		//Grab the initial offset 
		int offset  = fieldKey.length();

		//Store size of line
		int sizeOfLine = line.length();

		//Iterate through the line until we break
		while(true){

			//If the index is -1, the fieldKey did not appear
			//If the index+offset > size of line, we have reached end of file
			if (index == -1  || (index+offset > sizeOfLine-1))  break;

			//If the current character is '|', we have reached the 
			//end of that field value
			if (line.charAt(index+offset) == '|') break;

			//Once we get here, we can safely add our current char to result
			result += line.charAt(index + offset);

			//Increment our offset
			offset++;
		}

		//Return final field value
		return result;
	}

}
