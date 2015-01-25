/***
 * @author biprade
 * Date: 15-Nov-2014
 * NOTE: This file contains queries for EMBEDDED data model
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

public class queryModel2 {
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
					System.out.println("Total Execution time for QUERY 1 for good :"+totalTime+" milliseconds");
					
					long startTime1 = System.currentTimeMillis();
					System.out.println("Start Time : "+startTime1);
					executeQueryOne(" qwertyuiopasdfghjkl ");
					long endTime1   = System.currentTimeMillis();
					long totalTime1 = endTime1 - startTime1;
					System.out.println("Total Execution time for QUERY 1 for qwertyuiopasdfghjkl  : "+totalTime1+" milliseconds");
				
	
	
				 //QUERY 2
				 long startTime2 = System.currentTimeMillis();
				 System.out.println("Start Time : "+startTime2);
				 
				 executeQueryTwo();
				 
				 long endTime2   = System.currentTimeMillis();
				 long totalTime2 = endTime2 - startTime2;
				 
				 System.out.println("Total Execution time for QUERY 2: "+(totalTime2/1000)+" milliseconds");

				//QUERY 3
				 long startTime3 = System.currentTimeMillis();
				 System.out.println("Start Time : "+startTime3);
				 
				 executeQueryThree();
				 long endTime3   = System.currentTimeMillis();
				 long totalTime3 = endTime3 - startTime3;
				 System.out.println("Total Execution time for QUERY 3: "+totalTime3/1000+" seconds");
			 
				 //QUERY 4
				 long startTime4 = System.currentTimeMillis();
				 System.out.println("Start Time : "+startTime4);
				 executeQueryFour();
				 long endTime4   = System.currentTimeMillis();
				 long totalTime4 = endTime4 - startTime4;
				 System.out.println("Total Execution time for QUERY 4: "+totalTime4+" milli seconds");
}

private static void executeQueryOne(String word)
{
	
	Pattern searchWord=Pattern.compile(word); 
	DBCollection collection=db.getCollection("second_data_model");
	
	//Query the SECOND_DATA_MODEL collection for a given KEYWORD in the tweets.TEXT field
	DBObject projectionString=new BasicDBObject("_id",0).append("user_id",1);
	BasicDBObject queryString = new BasicDBObject();
	queryString.put("tweets.Text",searchWord);
	System.out.println("QUERY : db.second_data_model.find("+queryString+")");
	DBCursor cursor=collection.find(queryString,projectionString);
	 
	 
	 try{
	 while (cursor.hasNext())
	 {
		 System.out.println(cursor.next().get("user_id"));
		 
	 }
	 }
	 finally{
		 cursor.close();
	 }
	
}

private static void executeQueryTwo() {
	
	//db.second_data_model.aggregate([{$unwind:'$tweets'},{$match:{'tweets.Hashtags':{$ne:""}}},{$group:{_id:"",cumulatedRetweetCount:{$sum:'$tweets.RetCount'}}},{$project:{cumulatedRetweetCount:1,_id:0}}])
	DBCollection collection = db.getCollection("second_data_model");
	//QUERY 2
	
	//PIPELINE 1 {$unwind:'$tweets'}
	 DBObject unwindExpression=new BasicDBObject("$unwind","$tweets");
	 
	 //PIPELINE 2 : {$match:{Hashtags:{$ne:""}}}
	 DBObject queryExpressionForNonEmptyHashTag=new BasicDBObject("$ne","");
	 DBObject queryStringForNonEmptyHashTag=new BasicDBObject("tweets.Hashtags",queryExpressionForNonEmptyHashTag);
	 DBObject matchNonEmptyHashTagTweets=new BasicDBObject("$match",queryStringForNonEmptyHashTag);
	

	 
	 //PIPELINE 3: {$group:{_id:"",cumulatedRetweetCount:{$sum:"$RetCount"}}}
	 DBObject sumExpression= new BasicDBObject("$sum","$tweets.RetCount");
	 
	 DBObject groupExpression=new BasicDBObject("_id","").append("cumulatedRetweetCount",sumExpression);
	 DBObject groupQueryString = new BasicDBObject("$group",groupExpression);
	 
	 
	 //PIPELINE 4: {$project:{cumulatedRetweetCount:1,_id:0}}
	 DBObject finalOutput=new BasicDBObject("_id",0).append("cumulatedRetweetCount",1);
	 DBObject projectSpecifiedFinalFields = new BasicDBObject("$project",finalOutput);
	 
	 System.out.println("QUERY : db.second_data_model.aggregate("+"[ "+unwindExpression.toString()+","+matchNonEmptyHashTagTweets.toString()+", "+", "+groupQueryString.toString()+", "+projectSpecifiedFinalFields.toString()+" ])");
	// run aggregation
	 List<DBObject> pipeline = Arrays.asList(unwindExpression,matchNonEmptyHashTagTweets, groupQueryString, projectSpecifiedFinalFields);
	 AggregationOutput output = collection.aggregate(pipeline);
	 System.out.println("Result : "+output.results());
}

private static void executeQueryThree() {
	DBCollection dataModelTwoCollection=db.getCollection("second_data_model");
	//Query second_data_model collection to find the user with the highest value for  follower_count field
	DBObject sortString=new BasicDBObject("follower_count",-1);
	DBCursor cursor = dataModelTwoCollection.find().sort(sortString).limit(1);
	Object userid = null;
	if(cursor.hasNext())
	{	DBObject result=cursor.next();
		userid=result.get("user_id");
		System.out.println("USER ID "+userid+" has the highest number of followers ("+result.get("follower_count")+")");
	}
	//Query second_data_model collection to find the follower_ids of the user found above
	 
	 System.out.println("\n Follower names of the user with USER ID "+userid+ " :\n");
	 DBObject findFollowerQueryString;
	 if (userid.getClass()==Integer.class)
		 findFollowerQueryString=new BasicDBObject("user_id",(Integer)userid);
	 else
		 findFollowerQueryString=new BasicDBObject("user_id",(Long)userid);
	 DBObject projectionQueryString=new BasicDBObject("_id",0).append("follower_ids",1);
	 cursor = dataModelTwoCollection.find(findFollowerQueryString,projectionQueryString);
	 //Retrieve the follower_ids list 
	 List followerIDs=new ArrayList();
	 
	 
	 try {
		   if(cursor.hasNext()) {

			   followerIDs=(List) cursor.next().get("follower_ids");
			 
		   }
		  
	 	}
	 finally {
		   cursor.close();
		}
	
	 //Loop over each follower_id in the list retrieved above and find their name 
	 DBObject projectionsString=new BasicDBObject("_id",0).append("user_name", 1);
	 String userName;
	 DBObject findUserName;
	 for(Object userid1:followerIDs)
	 {
		 if (userid1.getClass()==Integer.class)
			 {findUserName=new BasicDBObject("user_id",(Integer)userid1);}
		 	 
		 else
			 {findUserName=new BasicDBObject("user_id",(Long)userid1);}
		 	 
		 
		 cursor=dataModelTwoCollection.find(findUserName,projectionsString);
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
					
					DBCollection dataModelTwoCollection=db.getCollection("second_data_model");
					//Query to select any random documents from second_data_model collection
					DBCursor cursor=dataModelTwoCollection.find().limit(100).skip((int) (Math.random() * dataModelTwoCollection.count()));
					Long userID1=null;
					Long userID2=null;
					Integer friendCount = null;
					Integer followerCount=null;
					try
					{
					if (cursor.hasNext())
					{
						//Make the first user document retrieved as the follower of the second user document	
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
					
					dataModelTwoCollection.update(searchString,updateString);
					
					//Increasing the follower count of userid2 by 1
					DBObject updateFollowerCount=new BasicDBObject("follower_count",followerCount+1); 
					updateString=new BasicDBObject("$set",updateFollowerCount); 
					searchString=new BasicDBObject("user_id",userID2); 
					
					dataModelTwoCollection.update(searchString,updateString);
					
					//insert a new element in the follower_ids field of userid2's document
					
					searchString=new BasicDBObject("user_id",userID2); 
					DBObject updateFollowerList=new BasicDBObject("follower_ids",userID1);
					updateString=new BasicDBObject("$push",updateFollowerList); 
					dataModelTwoCollection.update(searchString,updateString);
					
					//Print the new updated friend and follower count of user id 1 and user id 2 respectively
					DBObject queryString=new BasicDBObject("user_id",userID1);
					cursor=dataModelTwoCollection.find(queryString);
					if(cursor.hasNext())
						System.out.println(" Updated Friend count of USERD ID1 "+userID1+" is :"+cursor.next().get("friend_count"));
					queryString=new BasicDBObject("user_id",userID2);
					cursor=dataModelTwoCollection.find(queryString);
					if(cursor.hasNext())
						System.out.println(" Updated Follower count of USERD ID2 "+userID2+" is :"+cursor.next().get("follower_count"));
					

}



}




