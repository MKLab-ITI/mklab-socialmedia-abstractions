package gr.iti.mklab.framework.retrievers.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

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
import com.google.api.services.plus.Plus.People.Search;
import com.google.api.services.plus.PlusRequestInitializer;
import com.google.api.services.plus.model.Activity;
import com.google.api.services.plus.model.ActivityFeed;
import com.google.api.services.plus.model.PeopleFeed;
import com.google.api.services.plus.model.Person;
import com.restfb.util.StringUtils;

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
	
	private Plus googlePlusService;
	private String GooglePlusKey;
	
	public GooglePlusRetriever(Credentials credentials) {
		super(credentials);
		
		GooglePlusKey = credentials.getKey();
		
		GoogleCredential credential = new GoogleCredential();
		googlePlusService = new Plus.Builder(transport, jsonFactory, credential)
						.setApplicationName("SocialSensor")
						.setHttpRequestInitializer(credential)
						.setPlusRequestInitializer(new PlusRequestInitializer(GooglePlusKey)).build();
	}

	@Override
	public Response retrieveAccountFeed(AccountFeed feed, Integer maxRequests) {
		
		List<Item> items = new ArrayList<Item>();
		
		Date sinceDate = new Date(feed.getSinceDate());
		String label = feed.getLabel();
		
		int numberOfRequests = 0;
		
		String userID = feed.getId();
		String uName = feed.getUsername();
		if(uName == null && userID == null) {
			logger.info("#GooglePlus : No account feed");
			Response response = getResponse(items, numberOfRequests);
			return response;
		}
				
		//Retrieve userID from Google+
		StreamUser streamUser = null;
		try {
			if(userID == null) {
				// userid is not available. search with username
				Search searchPeople = googlePlusService.people().search(uName);
				searchPeople.setMaxResults(50L);
				
				PeopleFeed peopleFeed = searchPeople.execute();
				numberOfRequests++;

				List<Person> personsFound = peopleFeed.getItems();
				for(Person person : personsFound) {
					if(person.getUrl().equals("https://plus.google.com/+" + uName) || person.getDisplayName().equals(uName)) {
						userID = person.getId();
						streamUser = getStreamUser(userID);
						break;
					}
				}
			}
			else {
				numberOfRequests++;
				streamUser = getStreamUser(userID);
				uName = streamUser.getUsername();
			}
		} catch (Exception e) {
			logger.error(e);
			Response response = getResponse(items, numberOfRequests);
			return response;
		}

		if(streamUser == null) {
			logger.error("User not found. Feed: (" + feed.getId() + "");
			
			Response response = getResponse(items, numberOfRequests);
			return response;
		}
				
		//Retrieve activity with userID
		logger.info("Get public feed of user " + userID);
		boolean isFinished = false, sinceDateReached = false;
		while(true) {
			try {		
				Plus.Activities.List userActivities = googlePlusService.activities().list(userID, "public");
				userActivities.setMaxResults(100L);
				
				ActivityFeed activityFeed = userActivities.execute();
				numberOfRequests ++;
				
				List<Activity> activities = activityFeed.getItems();
				if(activities == null) {
					isFinished = true;
					break;
				}
				
				for (Activity activity : activities) {					
					if(activity == null || activity.getId() == null) {
						isFinished = true;
						break;
					}
					
					DateTime publicationTime = activity.getPublished();
					Date publicationDate = new Date(publicationTime.getValue());
					
					if(publicationDate.before(sinceDate)) {
						sinceDateReached = true;
						break;
					}
					
					String verb = activity.getVerb();
					String objectType = activity.getObject().getObjectType();
					if(!verb.equals("post") && !verb.equals("share") && !objectType.equals("note") && !objectType.equals("activity")) {
						// unknown type of activity
						continue;
					}
					
					Item googlePlusItem = new GooglePlusItem(activity, streamUser);
					if(label != null) {
						googlePlusItem.addLabel(label);
					}
					if(streamUser != null) {
						googlePlusItem.setStreamUser(streamUser);
					}
						
					items.add(googlePlusItem);
				}
				
				if(sinceDateReached) {
					logger.info("Stop retriever. Since date " + sinceDate + " reached for " + userID + " (" + uName + ").");
					break;
				}
				
				if(numberOfRequests > maxRequests) {
					logger.info("Stop retriever. Number of requests (" + numberOfRequests + ") has reached for " + userID + " (" + uName + ").");
					break;
				}
				
				if(activityFeed.getNextPageToken() == null) {
					logger.info("Stop retriever. There is no more pages to fetch for " + userID + " (" + uName + ").");
					break;
				}
				
				if(isFinished) {
					logger.info("Stop retriever. Activity is null for " + userID + " (" + uName + ").");
					break;
				}
				
				userActivities.setPageToken(activityFeed.getNextPageToken());
				
			} catch (IOException e) {
				logger.error("#GooglePlus Exception for feed (" + feed.getId() + ")", e);
				Response response = getResponse(items, numberOfRequests);
				return response;
			}
		}
		
		Response response = getResponse(items, numberOfRequests);
		return response;
	}
	
	@Override
	public Response retrieveKeywordsFeed(KeywordsFeed feed, Integer maxRequests) {
		
		List<Item> items = new ArrayList<Item>();
		int numberOfRequests = 0;
	
		Date sinceDate = new Date(feed.getSinceDate());
		String label = feed.getLabel();
		
		List<String> keywords = feed.getKeywords();
		if(keywords == null || keywords.isEmpty()) {
			logger.info("#GooglePlus : No keywords feed");
			Response response = getResponse(items, numberOfRequests);
			return response;
		}
		
		String tagsQuery = "";
		for(String key : keywords) {
			String [] words = key.split(" ");
			for(String word : words) {
				if(!tagsQuery.contains(word) && word.length() > 1)
					tagsQuery += word.toLowerCase() + " ";
			}
		}
		tagsQuery = tagsQuery.trim();
		
		if(tagsQuery.equals("")) {
			Response response = getResponse(items, numberOfRequests);
			return response;
		}
		
		logger.info("Search for (" + tagsQuery + ")");
		boolean isFinished = false, sinceDateReached = false;
		while(true) {
			try {
				Plus.Activities.Search searchActivities = googlePlusService.activities().search(tagsQuery);
				searchActivities.setMaxResults(20L);
				searchActivities.setOrderBy("recent");
				
				ActivityFeed activityFeed = searchActivities.execute();
				numberOfRequests++;
				
				List<Activity> activities = activityFeed.getItems();
				for (Activity activity : activities) {
					DateTime publicationTime = activity.getPublished();
					Date publicationDate = new Date(publicationTime.getValue());
						
					if(publicationDate.before(sinceDate)) {
						sinceDateReached = true;
						break;
					}
					
					String verb = activity.getVerb();
					String objectType = activity.getObject().getObjectType();
					if(!verb.equals("post") && !verb.equals("share") && !objectType.equals("note") && !objectType.equals("activity")) {
						// unknown type of activity
						continue;
					}

					Item googlePlusItem = new GooglePlusItem(activity);
					if(label != null) {
						googlePlusItem.addLabel(label);
					}

					items.add(googlePlusItem);
					
				}
				
				if(sinceDateReached) {
					logger.info("Stop retriever. Since date " + sinceDate + " reached for (" + tagsQuery + ").");
					break;
				}
				
				if(numberOfRequests > maxRequests) {
					logger.info("Stop retriever. Number of requests (" + numberOfRequests + ") has reached for (" + tagsQuery + ").");
					break;
				}
				
				if(activityFeed.getNextPageToken() == null) {
					logger.info("Stop retriever. There is no more pages to fetch for (" + tagsQuery + ").");
					break;
				}
				
				if(isFinished) {
					logger.info("Stop retriever. Activity is null for (" + tagsQuery + ").");
					break;
				}
				
				searchActivities.setPageToken(activityFeed.getNextPageToken());
				
			} catch (IOException e) {
				logger.error(e);
			}
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
		if(googlePlusService != null) {
			googlePlusService = null;
		}
	}
	
	@Override
	public MediaItem getMediaItem(String id) {
		return null;
	}
	
	@Override
	public StreamUser getStreamUser(String uid) {
		People people = googlePlusService.people();
		try {
			Get getRequest = people.get(uid);
			Person person = getRequest.execute();
			StreamUser streamUser = new GooglePlusStreamUser(person);
			return streamUser;
		} catch (IOException e) {
			logger.error("Exception for user " + uid, e);
		}
		return null;
	}
	
	
	public static void main(String...args) {
		
		String uid = "103783079217258362060";
		Date since = new Date(System.currentTimeMillis()-365*24*3600000l);
		
		AccountFeed aFeed = new AccountFeed(uid, null, since.getTime(), "GooglePlus");
		KeywordsFeed kFeed = new KeywordsFeed("id", "#debt", since.getTime(), "GooglePlus");
		
		Credentials credentials = new Credentials();
		credentials.setKey("xxxxxxxxxxxxxxxxxxxxxxxxxxx");
		
		
		GooglePlusRetriever retriever = new GooglePlusRetriever(credentials);
		
		Response response = retriever.retrieveAccountFeed(aFeed, 1);
		System.out.println(response.getNumberOfItems() + " items found for " + aFeed.getId());
		Response response2 = retriever.retrieveKeywordsFeed(kFeed, 1);
		for(Item item : response2.getItems()) {
			System.out.println(StringUtils.join(item.getTags()));
		}
	}
	
}
