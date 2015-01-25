/***
 * @author biprade
 * Date: 10-Nov-2014
 * NOTE: This file contains queries for REFERENCE data model
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class queryMongoDB {
	private static DB db;
	
public static void main(String[] args) throws IOException
{
	// To directly connect to a single MongoDB server
	
	MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
	
	//Name of the database is BIGDATA
	db = mongoClient.getDB( "BIGDATA" );
	
				//QUERY 1
					long startTime = System.currentTimeMillis();
					System.out.println("Start Time : "+startTime);
					executeQueryOne(" good ");
					long endTime   = System.currentTimeMillis();
					long totalTime = endTime - startTime;
					System.out.println("Total Execution time for QUERY 1 for good :"+(totalTime/60000)+" minutes");
					
					long startTime1 = System.currentTimeMillis();
					System.out.println("Start Time : "+startTime1);
					executeQueryOne(" qwertyuiopasdfghjkl ");
					long endTime1   = System.currentTimeMillis();
					long totalTime1 = endTime1 - startTime1;
					System.out.println("Total Execution time for QUERY 1 for qwertyuiopasdfghjkl  : "+(totalTime1/60000)+" minutes");
					
	
	
				 //QUERY 2
				 long startTime2 = System.currentTimeMillis();
				 System.out.println("Start Time : "+startTime2);
				 
				 executeQueryTwo();
				 
				 long endTime2   = System.currentTimeMillis();
				 long totalTime2 = endTime2 - startTime2;
				 
				 System.out.println("Total Execution time for QUERY 2: "+(totalTime2/60000)+" minutes");
			 

				//QUERY 3
				 long startTime3 = System.currentTimeMillis();
				 System.out.println("Start Time : "+startTime3);
				 //db.users.find().sort({follower_count:-1}).limit(1)
				 executeQueryThree();
				 long endTime3   = System.currentTimeMillis();
				 long totalTime3 = endTime3 - startTime3;
				 System.out.println("Total Execution time for QUERY 3: "+(totalTime3/60000)+" minutes");
				 
				 //QUERY 4
				 long startTime4 = System.currentTimeMillis();
				 System.out.println("Start Time : "+startTime4);
				 executeQueryFour();
				 long endTime4   = System.currentTimeMillis();
				 long totalTime4 = endTime4 - startTime4;
				 System.out.println("Total Execution time for QUERY 4: "+(totalTime4/1000)+" seconds");
}

private static void executeQueryOne(String word)
{
	Pattern searchWord=Pattern.compile(word); 
	DBCollection collection=db.getCollection("tweets_first_data_model");
	 //Query the tweets_first_data_model for a given KEYWORD in the TEXT field
	 DBObject projectionString=new BasicDBObject("_id",0).append("UserID",1);
	 BasicDBObject queryString = new BasicDBObject();
	 queryString.put("Text",searchWord);
	 System.out.println("QUERY : db.tweets_first_data_model.find("+queryString+")");
	 DBCursor cursor=collection.find(queryString,projectionString);
	 try{
	 while (cursor.hasNext())
	 {
		 System.out.println(cursor.next().get("UserID"));
		 
	 }
	 }
	 finally{
		 cursor.close();
	 }
	
}

private static void executeQueryTwo() {
	
	DBCollection collection = db.getCollection("tweets_first_data_model");
	//QUERY 2
	 //PIPELINE 1 : {$match:{Hashtags:{$ne:""}}}
	 DBObject queryExpressionForNonEmptyHashTag=new BasicDBObject("$ne","");
	 DBObject queryStringForNonEmptyHashTag=new BasicDBObject("Hashtags",queryExpressionForNonEmptyHashTag);
	 DBObject matchNonEmptyHashTagTweets=new BasicDBObject("$match",queryStringForNonEmptyHashTag);
	 
	 
	 //PIPELINE 2 : {$project:{_id:0,RetCount:1}}
	 DBObject fieldsToOutput=new BasicDBObject("_id",0).append("RetCount",1);
	 DBObject projectSpecifiedFields = new BasicDBObject("$project",fieldsToOutput);
	 
	 
	 //PIPELINE 3: {$group:{_id:"",cumulatedRetweetCount:{$sum:"$RetCount"}}}
	 DBObject sumExpression= new BasicDBObject("$sum","$RetCount");
	 //DBObject cumulatedRetweetCountExpression = new BasicDBObject("cumulatedRetweetCount",sumExpression);
	 DBObject groupExpression=new BasicDBObject("_id","").append("cumulatedRetweetCount",sumExpression);
	 DBObject groupQueryString = new BasicDBObject("$group",groupExpression);
	 
	 
	 //PIPELINE 4: {$project:{cumulatedRetweetCount:1,_id:0}}
	 DBObject finalOutput=new BasicDBObject("_id",0).append("cumulatedRetweetCount",1);
	 DBObject projectSpecifiedFinalFields = new BasicDBObject("$project",finalOutput);
	 
	 System.out.println("QUERY : db.tweets_first_data_model.aggregate("+"[ "+matchNonEmptyHashTagTweets.toString()+", "+projectSpecifiedFields.toString()+", "+groupQueryString.toString()+", "+projectSpecifiedFinalFields.toString()+" ])");
	
	 // run aggregation
	 List<DBObject> pipeline = Arrays.asList(matchNonEmptyHashTagTweets, projectSpecifiedFields, groupQueryString, projectSpecifiedFinalFields);
	 AggregationOutput output = collection.aggregate(pipeline);
	 System.out.println("Result : "+output.results());
}

private static void executeQueryThree() {
	DBCollection userCollection=db.getCollection("users");
	DBObject sortString=new BasicDBObject("follower_count",-1);
	DBCursor cursor = userCollection.find().sort(sortString).limit(1);
	Long userid = null;
	
	//Query USERS collection to find the user with the highest value for  follower_count field
	if(cursor.hasNext())
	{	DBObject result=cursor.next();
		userid=(Long) result.get("user_id");
		System.out.println("USER ID "+userid+" has the highest number of followers ("+result.get("follower_count")+")");
	}
	System.out.println("\n Follower names of the user with USER ID "+userid+ " :\n");
	
	//Query NETWORK collection to find the follower_IDs of the user found above
	 //db.network.find({USER_ID2:14230524},{_id:0,USER_ID1:1})
	 DBCollection networkCollection = db.getCollection("network");
	 DBObject findFollowerQueryString=new BasicDBObject("USER_ID2",userid);
	 DBObject projectionQueryString=new BasicDBObject("_id",0).append("USER_ID1",1);
	 cursor = networkCollection.find(findFollowerQueryString,projectionQueryString);
	 Long userID1;
	 //List to store all the follower_ids of the user
	 List<Long> userID1s=new ArrayList<Long>();
	 
	 
	 try {
		   while(cursor.hasNext()) {
			   userID1=Long.parseLong(cursor.next().get("USER_ID1").toString());
			   userID1s.add(userID1);
			   
		   }
		  
	 	}
	 finally {
		   cursor.close();
		}
	
	 DBObject projectionsString=new BasicDBObject("_id",0).append("user_name", 1);
	 String userName;
	 //Iterating over the follower_ids list prepared above
	 for(Long userid1:userID1s)
	 {
		 //Query USERS collection to find the name of the followers
		 DBObject findUserName=new BasicDBObject("user_id",userid1);
		 cursor=userCollection.find(findUserName,projectionsString);
		 try{
		 if(cursor.hasNext())
			 {userName = cursor.next().get("user_name").toString();
		  	  System.out.println(userName);
			 }
		 }
		 finally{
			 cursor.close();
		 }
		   
		 
	 }
	 
}

private static void executeQueryFour()
{
					DBCollection userCollection=db.getCollection("users");
					//Query to select any random documents from USERS collection
					DBCursor cursor=userCollection.find().limit(100).skip((int) (Math.random() * userCollection.count()));
					Long userID1=null;
					Long userID2=null;
					Integer friendCount = null;
					Integer followerCount=null;
					try
					{
					//Make the first user document retrieved as the follower of the second user document	
					if (cursor.hasNext())
					{
						DBObject result = cursor.next();
						userID1=(Long)(result.get("user_id"));
						friendCount=(Integer)result.get("friend_count");
						System.out.println(" Friend count of USERD ID1 "+userID1+" is :"+friendCount);
						
					}
					if(cursor.hasNext())
					{
						DBObject result1 = cursor.next();
						userID2=(Long)(result1.get("user_id"));
						followerCount=(Integer)result1.get("follower_count");
						System.out.println(" Follower count of USERD ID2 "+userID2+" is :"+followerCount);
					}
					}
					finally {
						cursor.close();
					}
					//Adding the first user id "userID1" as a follower of the second user id "userID2"
					
					//update the users collection
					//Increasing the friend count of userid1 by 1
					DBObject updateFriendCount=new BasicDBObject("friend_count",friendCount+1); 
					DBObject updateString=new BasicDBObject("$set",updateFriendCount); 
					DBObject searchString=new BasicDBObject("user_id",userID1); 
					
					userCollection.update(searchString,updateString);
					
					//Increasing the follower count of userid2 by 1
					DBObject updateFollowerCount=new BasicDBObject("follower_count",followerCount+1); 
					updateString=new BasicDBObject("$set",updateFollowerCount); 
					searchString=new BasicDBObject("user_id",userID2); 
					
					userCollection.update(searchString,updateString);
					
					//insert a new document into NETWORK collection i.e. add userid1 as the follower of userid2
					DBCollection networkCollection=db.getCollection("network");
					DBObject insertDocument=new BasicDBObject("USER_ID1",userID1).append("USER_ID2", userID2);
					networkCollection.insert(insertDocument);
					
					//Print the new updated friend and follower count of user id 1 and user id 2 respectively
					DBObject queryString=new BasicDBObject("user_id",userID1);
					cursor=userCollection.find(queryString);
					if(cursor.hasNext())
						System.out.println(" Updated Friend count of USERD ID1 "+userID1+" is :"+cursor.next().get("friend_count"));
					queryString=new BasicDBObject("user_id",userID2);
					cursor=userCollection.find(queryString);
					if(cursor.hasNext())
						System.out.println(" Updated Follower count of USERD ID2 "+userID2+" is :"+cursor.next().get("follower_count"));
					

}



}




