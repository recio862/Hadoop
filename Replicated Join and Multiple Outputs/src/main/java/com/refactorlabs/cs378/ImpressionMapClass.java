package com.refactorlabs.cs378;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * @author Renato John Recio
 * 
 * Mapper for UserSessions. Extends class Mapper, provided by Hadoop.
 * Mapper will take User sessions and output session objects
 * as values and the user id and apikey as the key. 
 */
public class ImpressionMapClass extends Mapper<LongWritable, Text, Text, AvroValue<Session>> {


	/**
	 * Local variable "word" will contain the key for our output
	 * The key is simply a concatenation of the apikey and userid
	 * 	 
	 * */
	private Text word = new Text();
	private Map<String, String> zipDma;

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		//Convert our text to string
		String line = value.toString();

		//Create our session and impression builders
		Session.Builder sessionBuilder = Session.newBuilder();
		Impression.Builder impressionBuilder = Impression.newBuilder();

		//////////////////////////////////////////////////////////
		//Initialize all of our avro fields
		String impType = getFieldValue("|type:", line).toUpperCase();
		String address = getFieldValue("address:", line);
		String city = getFieldValue("city:", line);
		String zip = getFieldValue("|zip:", line);
		if (zip.length() == 0)
			zip = getFieldValue("|listingzip:", line);
		String state = getFieldValue("state:", line);
		String lat = getFieldValue("lat:", line);
		String total = getFieldValue("total:", line);
		String startIndex = getFieldValue("start_index:", line);
		String timeStamp = getFieldValue("timestamp:", line);
		String idnumbers = getFieldValue("|id:", line);
		idnumbers = idnumbers.replaceAll(",", " ");
		String ab = getFieldValue("ab:", line);
		String actionName = getFieldValue("action_name:", line).toUpperCase();
		String resolution = getFieldValue("res:", line);
		String uagent = getFieldValue("uagent:", line);
		String apikey = getFieldValue("apikey:", line);
		String uid= getFieldValue("uid:", line);
		String domain = getFieldValue("domain:", line);
		String lon = getFieldValue("lon:", line);
		//////////////////////////////////////////////////////////

		//////////////////////////////////////////////////////////
		//Set all of our non-enumerated session field values
		sessionBuilder.setApiKey(getValidatedFieldValue(apikey));
		sessionBuilder.setUserId(getValidatedFieldValue(uid));
		sessionBuilder.setResolution(getValidatedFieldValue(resolution));
		sessionBuilder.setUserAgent(getValidatedFieldValue(uagent));
		//////////////////////////////////////////////////////////

		//////////////////////////////////////////////////////////
		//Set all of our impression field values
		impressionBuilder.setAb(getValidatedFieldValue(ab));
		impressionBuilder.setLon(parseValidDouble(lon));
		impressionBuilder.setLat(parseValidDouble(lat));
		impressionBuilder.setAddress(getValidatedFieldValue(address));
		impressionBuilder.setCity(getValidatedFieldValue(city));
		impressionBuilder.setZip(getValidatedFieldValue(zip));
		impressionBuilder.setState(getValidatedFieldValue(state));
		impressionBuilder.setId(getIdList(idnumbers));
		impressionBuilder.setStartIndex(parseValidInt(startIndex));
		impressionBuilder.setTotal(parseValidInt(total));
		impressionBuilder.setDomain(getValidatedFieldValue(domain));
		impressionBuilder.setTimestamp(parseValidLong(timeStamp));
		//////////////////////////////////////////////////////////

		//////////////////////////////////////////////////////////
		//Set the dma for our impression
		String zipKey = getValidatedFieldValue(zip); //Get validated zip
		//If zip is not null, we check in our map
		if (zipKey != null){
			//Check map to see if it contains zip
			if (zipDma.containsKey(zipKey)) //if so, set to the map's dma
				impressionBuilder.setDma(zipDma.get(zipKey));
			else
				impressionBuilder.setDma(null); //else set to null
		}
		else //zip is null, set DMA to null
			impressionBuilder.setDma(null);

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
		//Set all of our impression field values that are enumerated
		//Simply check to see if string exists in the input text
		//And set enumerations based on string
		if (line.contains("action:"))
			impressionBuilder.setAction(Action.CLICK);
		else
			impressionBuilder.setAction(Action.PAGE_VIEW);
		if (actionName.contains("PAGE"))
			impressionBuilder.setActionName(ActionName.DEALER_PAGE_VIEWED);
		else if (actionName.contains("WEBSITE"))
			impressionBuilder.setActionName(ActionName.DEALER_WEBSITE_VIEWED);
		else if (actionName.contains("UNHOSTED"))
			impressionBuilder.setActionName(ActionName.VIEWED_CARFAX_REPORT_UNHOSTED);
		else if (actionName.contains("CARFAX"))
			impressionBuilder.setActionName(ActionName.VIEWED_CARFAX_REPORT);
		else if (actionName.contains("MORE"))
			impressionBuilder.setActionName(ActionName.MORE_PHOTOS_VIEWED);
		else
			impressionBuilder.setActionName(ActionName.NONE);
		if (getFieldValue("vertical:", line).toUpperCase().equals("OTHER"))
			impressionBuilder.setVertical(Vertical.OTHER);
		else
			impressionBuilder.setVertical(Vertical.CARS);

		if (impType.length() == 0)
			impressionBuilder.setImpressionType(ImpressionType.SRP);
		else if (impType.contains("ACTION"))
			impressionBuilder.setImpressionType(ImpressionType.ACTION);
		else if (impType.contains("THANKYOU"))
			impressionBuilder.setImpressionType(ImpressionType.THANK_YOU);
		else
			impressionBuilder.setImpressionType(ImpressionType.VDP);

		if (line.contains("phone_type:tracked"))
			impressionBuilder.setPhoneType(PhoneType.TRACKED);
		//////////////////////////////////////////////////////////


		//Set our output key using concatenated uid and apikey
		word.set(uid+":"+apikey);


		//Create impList out of our impression and store in the session
		List<Impression> impList = new ArrayList<Impression>();
		impList.add(impressionBuilder.build());
		sessionBuilder.setImpressions(impList);

		List<Lead> leadList = new ArrayList<Lead>();
		sessionBuilder.setLeads(leadList);

		//Write key and session (wrapped in avro value) to context
		context.write( word, new AvroValue<Session>(sessionBuilder.build()));


	}

	//Parse valid double and return 0.0 if invalid
	private Double parseValidDouble(String key){
		if (getValidatedFieldValue(key) != null)
			return Double.parseDouble(key);
		else
			return 0.0;
	}

	//Parse valid long and return 0 if invalid
	private Long parseValidLong(String key){
		if (getValidatedFieldValue(key) != null)
			return Long.parseLong(key);
		else
			return (long)0;
	}

	//Parse valid integer and return 0 if invalid
	private Integer parseValidInt(String key) {
		if (getValidatedFieldValue(key) != null)
			return Integer.parseInt(key);
		else
			return 0;
	}

	//Takes a string of idnumbers and parses into a Long type ArrayList of Id's
	private List<Long> getIdList(String idnumbers) {
		//Tokenizer for the idnumbers
		StringTokenizer tokenizer = new StringTokenizer(idnumbers);
		List<Long> idList = new ArrayList<Long>(); //Stores our list of Id's
		while (tokenizer.hasMoreTokens()) {
			String token = tokenizer.nextToken();
			long temp = Long.parseLong(token); //Parse the current token
			idList.add(temp); //Store to list
		}
		return idList;
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
	private String getFieldValue(String fieldKey, String line){
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


	//Parse distributed cache files
	@Override
	public void setup(Context context) {

		//Initialize our map
		zipDma = new HashMap<String, String>();

		//Initialize our paths to null
		Path[] paths = null;


		//Attempt to get local cache files from context conf
		try {
			paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		} catch (IOException e) {
			e.printStackTrace();
		} 

		//If we have successfully retrieved cache files, proceed to parse their content
		if (paths != null){
			//For each path, parse their contents
			for (int i = 0 ; i < paths.length; i++){
				Scanner scanner = null;
				try {
					scanner = new Scanner(new File(paths[i].toString()));
					//While our scanner has next, we parse zip and dma
					while (scanner.hasNext()){
						String s = scanner.next();
						//Split the zip and dma into two strings
						String[] zipDmaStr = s.split(",");
						//Error check
						if (zipDmaStr.length == 2)
							//Store the zip and dma into our map
							zipDma.put(zipDmaStr[0], zipDmaStr[1]);
					}
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} 
			}
		}
	}

}
