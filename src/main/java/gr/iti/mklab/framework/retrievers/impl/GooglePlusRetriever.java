package gr.iti.mklab.framework.retrievers.impl;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.DateTime;
import com.google.api.services.plus.Plus;
import com.google.api.services.plus.Plus.People;
import com.google.api.services.plus.Plus.People.Get;
import com.google.api.services.plus.PlusRequestInitializer;
import com.google.api.services.plus.model.Activity;
import com.google.api.services.plus.model.ActivityFeed;
import com.google.api.services.plus.model.PeopleFeed;
import com.google.api.services.plus.model.Person;

import gr.iti.mklab.framework.Credentials;
import gr.iti.mklab.framework.abstractions.socialmedia.items.GooglePlusItem;
import gr.iti.mklab.framework.abstractions.socialmedia.users.GooglePlusStreamUser;
import gr.iti.mklab.framework.common.domain.Item;
import gr.iti.mklab.framework.common.domain.MediaItem;
import gr.iti.mklab.framework.common.domain.StreamUser;
import gr.iti.mklab.framework.common.domain.feeds.AccountFeed;
import gr.iti.mklab.framework.common.domain.feeds.GroupFeed;
import gr.iti.mklab.framework.common.domain.feeds.KeywordsFeed;
import gr.iti.mklab.framework.common.domain.feeds.LocationFeed;
import gr.iti.mklab.framework.retrievers.Response;
import gr.iti.mklab.framework.retrievers.SocialMediaRetriever;

/**
 * Class responsible for retrieving Google+ content based on keywords or google+ users
 * The retrieval process takes place through Google API
 * 
 * @author manosetro
 * @email  manosetro@iti.gr
 */
public class GooglePlusRetriever extends SocialMediaRetriever {
	
	private Logger logger = LogManager.getLogger(GooglePlusRetriever.class);
	
	private static final HttpTransport transport = new NetHttpTransport();
	private static final JsonFactory jsonFactory = new JacksonFactory();
	
	private Plus plusSrv;
	private String userPrefix = "https://plus.google.com/+";
	private String GooglePlusKey;

	public GooglePlusRetriever(Credentials credentials) {
		super(credentials);
		
		GooglePlusKey = credentials.getKey();
		GoogleCredential credential = new GoogleCredential();
		plusSrv = new Plus.Builder(transport, jsonFactory, credential)
						.setApplicationName("SocialSensor")
						.setHttpRequestInitializer(credential)
						.setPlusRequestInitializer(new PlusRequestInitializer(GooglePlusKey)).build();
	}

	@Override
	public Response retrieveAccountFeed(AccountFeed feed, Integer maxRequests) {
		
		List<Item> items = new ArrayList<Item>();
		
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
		
		Date lastItemDate = new Date(feed.getSinceDate());
		String label = feed.getLabel();
		
		int numberOfRequests = 0;
		
		boolean isFinished = false;
		
		
		String userID = feed.getId();
		String uName = feed.getUsername();
		if(uName == null && userID == null) {
			logger.info("#GooglePlus : No source feed");
			Response response = getResponse(items, numberOfRequests);
			return response;
		}
				
		//Retrieve userID from Google+
		StreamUser streamUser = null;
		Plus.People.Search searchPeople = null;
		List<Person> people;
		List<Activity> pageOfActivities;
		ActivityFeed activityFeed;
		Plus.Activities.List userActivities;
		
		try {
			if(userID == null) {
				searchPeople = plusSrv.people().search(uName);
				searchPeople.setMaxResults(20L);
				PeopleFeed peopleFeed = searchPeople.execute();
				people = peopleFeed.getItems();
				for(Person person : people) {
					if(person.getUrl().compareTo(userPrefix+uName) == 0) {
						userID = person.getId();
						streamUser = getStreamUser(userID);
						break;
					}
				}
			}
			else {
				streamUser = getStreamUser(userID);
			}
			
			//Retrieve activity with userID
			logger.info("Get public feed of " + userID);
			userActivities = plusSrv.activities().list(userID, "public");
			userActivities.setMaxResults(20L);
			activityFeed = userActivities.execute();
			pageOfActivities = activityFeed.getItems();
			numberOfRequests ++;
			
		} catch (Exception e) {
			logger.error(e);
			Response response = getResponse(items, numberOfRequests);
			return response;
		}
		
		while(pageOfActivities != null && !pageOfActivities.isEmpty()) {
			try {
				for (Activity activity : pageOfActivities) {
				
					DateTime publicationTime = activity.getPublished();
					String PublicationTimeStr = publicationTime.toString();
					String newPublicationTimeStr = PublicationTimeStr.replace("T", " ").replace("Z", " ");
					
					Date publicationDate = null;
					try {
						publicationDate = (Date) formatter.parse(newPublicationTimeStr);
						
					} catch (ParseException e) {
						logger.error("#GooglePlus - ParseException: "+e);
						
						Response response = getResponse(items, numberOfRequests);
						return response;
					}
					
					if(publicationDate.after(lastItemDate) && activity != null && activity.getId() != null) {
						Item googlePlusItem = new GooglePlusItem(activity);
						if(label != null) {
							googlePlusItem.addLabel(label);
						}
						
						if(streamUser != null) {
							googlePlusItem.setStreamUser(streamUser);
						}
						
						items.add(googlePlusItem);
					}
					else {
						isFinished = true;
						break;
					}
					
				}
				numberOfRequests++;
				if(isFinished || numberOfRequests>maxRequests || (activityFeed.getNextPageToken() == null))
					break;
				 
				userActivities.setPageToken(activityFeed.getNextPageToken());
				activityFeed = userActivities.execute();
				pageOfActivities = activityFeed.getItems();
				
			} catch (IOException e) {
				logger.error("#GooglePlus Exception : "+e);
				Response response = getResponse(items, numberOfRequests);
				return response;
			}
			
			if(isFinished){
				break;
			}
		}
		
		Response response = getResponse(items, numberOfRequests);
		return response;
	}
	
	@Override
	public Response retrieveKeywordsFeed(KeywordsFeed feed, Integer maxRequests) {
		
		List<Item> items = new ArrayList<Item>();
		int numberOfRequests = 0;
		
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
		Date lastItemDate = new Date(feed.getSinceDate());
		String label = feed.getLabel();
		
		boolean isFinished = false;
		
		List<String> keywords = feed.getKeywords();
		
		if(keywords == null || keywords.isEmpty()) {
			logger.info("#GooglePlus : No keywords feed");
			Response response = getResponse(items, numberOfRequests);
			return response;
		}
		
		String tags = "";
		for(String key : keywords) {
			String [] words = key.split(" ");
			for(String word : words) {
				if(!tags.contains(word) && word.length() > 1)
					tags += word.toLowerCase()+" ";
			}
		}
		
		
		if(tags.equals("")) {
			Response response = getResponse(items, numberOfRequests);
			return response;
		}
		
		Plus.Activities.Search searchActivities;
		ActivityFeed activityFeed;
		List<Activity> pageOfActivities;
		try {
			searchActivities = plusSrv.activities().search(tags);
			searchActivities.setMaxResults(20L);
			searchActivities.setOrderBy("recent");
			activityFeed = searchActivities.execute();
			pageOfActivities = activityFeed.getItems();
			numberOfRequests++;
		} catch (IOException e1) {
			Response response = getResponse(items, numberOfRequests);
			return response;
		}
		
		Map<String, StreamUser> users = new HashMap<String, StreamUser>();
		
		while(pageOfActivities != null && !pageOfActivities.isEmpty()) {
			numberOfRequests++;
			for (Activity activity : pageOfActivities) {
				
				if(activity.getObject().getAttachments() != null){
					
					DateTime publicationTime = activity.getPublished();
					String PublicationTimeStr = publicationTime.toString();
					String newPublicationTimeStr = PublicationTimeStr.replace("T", " ").replace("Z", " ");
					
					Date publicationDate = null;
					try {
						publicationDate = (Date) formatter.parse(newPublicationTimeStr);
						
					} catch (ParseException e) {
						logger.error("#GooglePlus - ParseException: " + e.getMessage());
						
						Response response = getResponse(items, numberOfRequests);
						return response;
					}
					
					if(publicationDate.after(lastItemDate) && activity != null && activity.getId() != null) {
						Item googlePlusItem = new GooglePlusItem(activity);
						if(label != null) {
							googlePlusItem.addLabel(label);
						}
						
						String userID = googlePlusItem.getStreamUser().getUserid();
						StreamUser streamUser = null;
						if(userID != null && !users.containsKey(userID)) {
							streamUser = getStreamUser(userID);
							users.put(userID, streamUser);	
						}
						else {
							streamUser = users.get(userID);
						}
						
						if(streamUser != null) {
							googlePlusItem.setStreamUser(streamUser);
						}
						
						items.add(googlePlusItem);

					}
		
				}
		
			 }
			
			numberOfRequests++;

			 if(numberOfRequests>maxRequests || isFinished || (activityFeed.getNextPageToken() == null)) {
				 break;
			 }
			 
			 searchActivities.setPageToken(activityFeed.getNextPageToken());
			 try {
				activityFeed = searchActivities.execute();
			} catch (IOException e) {
				logger.error("GPlus Retriever Exception: " + e.getMessage());
			}
			 pageOfActivities = activityFeed.getItems();
		
		}
		
		Response response = getResponse(items, numberOfRequests);
		return response;
		
	}
	@Override
	public Response retrieveLocationFeed(LocationFeed feed, Integer maxRequests) {
		return new Response();
    }
	
	@Override
	public Response retrieveGroupFeed(GroupFeed feed, Integer maxRequests) {
		return new Response();
	}
	
	@Override
	public void stop() {
		if(plusSrv != null) {
			plusSrv = null;
		}
	}
	
	@Override
	public MediaItem getMediaItem(String id) {
		return null;
	}
	
	@Override
	public StreamUser getStreamUser(String uid) {
		
		People peopleSrv = plusSrv.people();
		try {
			Get getRequest = peopleSrv.get(uid);
			Person person = getRequest.execute();
			
			StreamUser streamUser = new GooglePlusStreamUser(person);
			
			return streamUser;
		} catch (IOException e) {
			logger.error("Exception for user " + uid);
		}
		
		return null;
	}
	
	
	public static void main(String...args) {
		String uid = "102155862500050097100";
		Date since = new Date(System.currentTimeMillis()-24*3600000);
		AccountFeed feed = new AccountFeed(uid, null, since.getTime(), "GooglePlus");
		
		Credentials credentials = new Credentials();
		credentials.setKey("AIzaSyB-knYzMRW6tUzobP-V1hTWYAXEps1Wngk");
		
		GooglePlusRetriever retriever = new GooglePlusRetriever(credentials);
		
		Response response = retriever.retrieveAccountFeed(feed, 1);
		for(Item item : response.getItems()) {
			System.out.println(item);
		}
	}
	
}
