/***
 * @author biprade
 * Date: 10-Nov-2014
 * This file prepares the TWEETS_FIRST DATA_MODEL Collection in REFERENCE data model
 * and the entire EMBEDDED DATA MODEL
 */
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.Bytes;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class LoadMongoDB  {
public static void main(String[] args) throws IOException
{
	 
	
	// To directly connect to a single MongoDB server
	MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
	
	//Create First Data Model and Measure the operation time
	//Name of the database is BIGDATA
	DB db = mongoClient.getDB( "BIGDATA" );
	long startTime = System.currentTimeMillis();
	System.out.println(startTime);
	createFirstDataModel(db);
	long endTime   = System.currentTimeMillis();
    long totalTime = endTime - startTime;
    System.out.println(totalTime);
    
  //Create Second Data Model and Measure the operation time
	long startTime1 = System.currentTimeMillis();
	System.out.println("Start Time : "+startTime1);
    createSecondDataModel(db);
    long endTime1   = System.currentTimeMillis();
    System.out.println("End Time : "+endTime1);
    long totalTime1 = endTime - startTime;
    System.out.println("Total time in minutes : "+(totalTime1/60000));
		}

private static void createSecondDataModel(DB db)
{
	
	DBCollection secondModelcollection = db.getCollection("second_data_model");
	DBCollection tweetsFirstModelcollection = db.getCollection("tweets_first_data_model");
	DBCollection usercollection = db.getCollection("users");
	DBCollection networkcollection = db.getCollection("network");
	DBObject projectionString=new BasicDBObject("_id",0);
	DBObject projectionString1=new BasicDBObject("_id",0).append("UserID", 0);
	DBCursor cursor=usercollection.find(new BasicDBObject(),projectionString).addOption(Bytes.QUERYOPTION_NOTIMEOUT);
	DBObject result=null;
	Object userID=null;
	DBObject userIDquery=null;
	//List to store all the follower_ids of a given user
	List followerIDs=new ArrayList();
	
	//List to store all the tweets of a given user
	List<HashMap> tweetsOfUser=new ArrayList<HashMap>();
	long count=0;
	long rowsToInsert=0;
	BulkWriteOperation builder = secondModelcollection.initializeOrderedBulkOperation();
	
	//Iterating over each record of USERS collection until 17160294 tweet documents are loaded
	while (cursor.hasNext() && count<=17160294)
	{
		
		followerIDs.clear();
		tweetsOfUser.clear();
		
		
		result=cursor.next();
		
		if(result.get("user_id").getClass()==Integer.class)
			userIDquery=new BasicDBObject("user_id",(Integer)result.get("user_id"));
		else
			userIDquery=new BasicDBObject("user_id",(Long)result.get("user_id"));
		
		
		DBCursor dataModel2Cursor=secondModelcollection.find(userIDquery);
		DBObject tweetUserQueryString=new BasicDBObject("UserID",result.get("user_id").toString());
		DBCursor tweetCursor=tweetsFirstModelcollection.find(tweetUserQueryString,projectionString1);
		
		//Only execute if the user document is not present in the second_data_model collection and the user has tweets in the TWEETS_FIRST_DATA_MODEL collection
		if((!dataModel2Cursor.hasNext()) && tweetCursor.hasNext() )
		{
			
		//Adding USERS collection docuement to a BSON Object
		DBObject document=new BasicDBObject(result.toMap());
		
		DBObject userQueryString=null;
		
		//Creating a list of all followers of the User ID from the network collection
		if(result.get("user_id").getClass()==Integer.class)
			userQueryString=new BasicDBObject("USER_ID2",(Integer)result.get("user_id"));
		else
			userQueryString=new BasicDBObject("USER_ID2",(Long)result.get("user_id"));
		DBCursor networkCursor=networkcollection.find(userQueryString);
		while (networkCursor.hasNext())
		{
			userID=networkCursor.next().get("USER_ID1");
			if (userID.getClass()==Integer.class)
				followerIDs.add((Integer) userID);
			else
				followerIDs.add((Long) userID);
		}
		//All the follower ids of a user from NETWORK collection are added to the BSON Object
		document.put("follower_ids",followerIDs);
		
		//Creating a list of tweets by a user
		while(tweetCursor.hasNext())
		{
			tweetsOfUser.add((HashMap) tweetCursor.next().toMap());
			count++;
		}
		
		//All the tweets of a user from TWEETS_FIRST_DATA_MODEL collection are added to the BSON Object
		document.put("tweets",tweetsOfUser);
		
		//Do a bulk insert after every 1000 BSON objects or documents are prepared
		if(rowsToInsert<=1000)
		{
			builder.insert(document);
			rowsToInsert++;
		}
		else
		{
			rowsToInsert=0;
			builder.execute();
			builder=secondModelcollection.initializeOrderedBulkOperation();
		}
		
	}
	}
}

private static void createFirstDataModel(DB db)
		throws IOException, FileNotFoundException, UnsupportedEncodingException {
	DBCollection collection = db.getCollection("tweets_first_data_model");
	
	String filePath="/Users/biprade/Downloads/tweets";
	Path dir = FileSystems.getDefault().getPath( filePath );
	
    DirectoryStream<Path> stream = Files.newDirectoryStream( dir );
    
    //Iterate over all tweet files present in the tweet directory
		for (Path path : stream) {
			
			File file = new File(path.toString());
			//Ignore the file if the file length is less than or equal to 0
			if (file.length() <= 0) 
				continue;
			
			BufferedInputStream bs = new BufferedInputStream(
					new FileInputStream(file));
			byte[] data = new byte[(int) file.length()];
			bs.read(data);
			bs.close();
			String content = new String(data, "UTF-8").trim();
			
			//Extract the tweets from a file in a string array
			content = "***\n" + content + "\n***\n";
			//System.out.println(path.toString());
			String fileName=path.getFileName().toString();
			String[] tweets = StringUtils.substringsBetween(content,
					"***\n***\n", "***\n***\n");
			String type;
			String origin;
			String text;
			String URL;
			String ID;
			String time;
			int retCount;
			String favourite;
			String mentionedEntities;
			String hashTags;
			
			//Loop over each tweet string in a file and extract the fields of a tweet
			for (String tweet : tweets) {
				// Prepare data for MongoDB
				type = StringUtils.substringBetween(tweet, "Type:", "Origin:")
						.trim();
				origin = StringUtils
						.substringBetween(tweet, "Origin:", "Text:").trim();
				text = StringUtils.substringBetween(tweet, "Text:", "URL:")
						.trim();
				URL = StringUtils.substringBetween(tweet, "URL:", "ID:").trim();
				ID = StringUtils.substringBetween(tweet, "ID:", "Time:").trim();
				time = StringUtils
						.substringBetween(tweet, "Time:", "RetCount:").trim();
				retCount = Integer.valueOf(StringUtils.substringBetween(tweet, "RetCount:",
						"Favorite:").trim());
				favourite = StringUtils.substringBetween(tweet, "Favorite:",
						"MentionedEntities:").trim();
				mentionedEntities = StringUtils.substringBetween(tweet,
						"MentionedEntities:", "Hashtags:").trim();
				hashTags = StringUtils.substringBetween(tweet + "$#@",
						"Hashtags:", "$#@").trim();
				
					
				 //Prepare BSON object
				 BasicDBObject document = new BasicDBObject("Type",type)
				 .append("Origin", origin)
				 .append("Text",text)
				 .append("URL", URL)
				 .append("ID",ID)
				 .append("Time",time)
				 .append("RetCount",retCount)
				 .append("Favorite",favourite)
				 .append("MentionedEntities",mentionedEntities)
				 .append("Hashtags",hashTags)
				 .append("UserID",fileName);
				 
				 //Insert it into MongoDB collection
				
				
				 collection.insert(document);
		
				
				
			}
		}

    stream.close();
    
}


}



