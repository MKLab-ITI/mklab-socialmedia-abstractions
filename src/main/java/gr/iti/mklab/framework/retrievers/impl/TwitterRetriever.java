package gr.iti.mklab.framework.retrievers.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import twitter4j.GeoLocation;
import twitter4j.Paging;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.RateLimitStatus;
import twitter4j.ResponseList;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.User;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;
import gr.iti.mklab.framework.Credentials;
import gr.iti.mklab.framework.abstractions.socialmedia.items.TwitterItem;
import gr.iti.mklab.framework.abstractions.socialmedia.users.TwitterStreamUser;
import gr.iti.mklab.framework.common.domain.Item;
import gr.iti.mklab.framework.common.domain.Location;
import gr.iti.mklab.framework.common.domain.MediaItem;
import gr.iti.mklab.framework.common.domain.StreamUser;
import gr.iti.mklab.framework.common.domain.feeds.AccountFeed;
import gr.iti.mklab.framework.common.domain.feeds.GroupFeed;
import gr.iti.mklab.framework.common.domain.feeds.KeywordsFeed;
import gr.iti.mklab.framework.common.domain.feeds.LocationFeed;
import gr.iti.mklab.framework.retrievers.Response;
import gr.iti.mklab.framework.retrievers.SocialMediaRetriever;

/**
 * Class responsible for retrieving Twitter content based on keywords, twitter users or locations
 * The retrieval process takes place through Twitter API (twitter4j).
 * 
 * @author Manos Schinas
 * @email  manosetro@iti.gr
 */
public class TwitterRetriever extends SocialMediaRetriever {
	
	private Logger  logger = Logger.getLogger(TwitterRetriever.class);
	private boolean loggingEnabled = true;
	
	private Twitter twitter = null;
	private TwitterFactory tf = null;
	
	public TwitterRetriever(Credentials credentials) {
		super(credentials);
		
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setJSONStoreEnabled(false)
			.setOAuthConsumerKey(credentials.getKey())
			.setOAuthConsumerSecret(credentials.getSecret())
			.setOAuthAccessToken(credentials.getAccessToken())
			.setOAuthAccessTokenSecret(credentials.getAccessTokenSecret());
		Configuration conf = cb.build();
		
		tf = new TwitterFactory(conf);
		twitter = tf.getInstance();
		
	}
	
	@Override
	public Response retrieveAccountFeed(AccountFeed feed, Integer requests) {
		
		Response response = new Response();
		List<Item> items = new ArrayList<Item>();
		
		int count = 200;
		
		Integer numberOfRequests = 0;
		
		Date sinceDate = new Date(feed.getSinceDate());
		Date newSinceDate = sinceDate;
		
		String label = feed.getLabel();

		String screenName = feed.getUsername();
		if(screenName == null) {
			response.setItems(items);
			response.setRequests(numberOfRequests);
			return response;
		}
		
		int page = 1;
		Paging paging = new Paging(page, count);
		boolean sinceDateReached = false;
		while(true) {
			try {
				ResponseList<Status> responseList = null;
				if(loggingEnabled) {
					logger.info("Retrieve timeline for " + screenName + ". Page: " + paging.getPage());
				}
				
				responseList = twitter.getUserTimeline(screenName, paging);
				
				numberOfRequests++;
				
				for(Status status : responseList) {
					if(status != null) {
						
						if(sinceDate != null) {
							Date createdAt = status.getCreatedAt();
							if(newSinceDate.before(createdAt)) {
								newSinceDate = new Date(createdAt.getTime());
							}
							if(sinceDate.after(createdAt)) {
								sinceDateReached = true;
								break;
							}
						}
						
						Item twitterItem = new TwitterItem(status);
						if(label != null) {
							twitterItem.addLabel(label);
						}
						
						items.add(twitterItem);
					}
				}
				
				if(numberOfRequests >= requests) {
					if(loggingEnabled)	
						logger.info("numberOfRequests: " + numberOfRequests + " > " + requests);
					break;
				}
				if(sinceDateReached) {
					if(loggingEnabled)
						logger.info("Since date reached: " + sinceDate);
					break;
				}
				
				paging.setPage(++page);
			} catch (TwitterException e) {
				logger.error(e);
				break;
			}
		}
		
		response.setItems(items);
		response.setRequests(numberOfRequests);
		return response;
	}
	
	@Override
	public Response retrieveKeywordsFeed(KeywordsFeed feed, Integer requests) {
			
		Response response = new Response();
		List<Item> items = new ArrayList<Item>();
		
		int count = 100;
		int numberOfRequests = 0;

		Date sinceDate = new Date(feed.getSinceDate());
		Date newSinceDate = sinceDate;
		
		String label = feed.getLabel();
		
		List<String> keywords = feed.getKeywords();
		if(keywords == null || keywords.isEmpty()) {
			logger.error("#Twitter : No keywords feed");
			return response;
		}
		
		String textQuery = StringUtils.join(keywords, " OR ");
		
		if(textQuery.equals("")) {
			return response;
		}
		
		//Set the query
		logger.info("Query String: " + textQuery + " with label=" + label);
	
		Query query = new Query(textQuery);
	
		//query.setUntil("2012-02-01");
		query.count(count);
		query.setResultType(Query.RECENT); //do not set last item date-causes problems!

		boolean sinceDateReached = false;
		try {
			if(loggingEnabled) {
				logger.info("Request for " + query);
			}
			
			QueryResult queryResult = twitter.search(query);
			
			while(queryResult != null) {
				numberOfRequests++;
				
				List<Status> statuses = queryResult.getTweets();
				
				if(statuses == null || statuses.isEmpty()) {
					if(loggingEnabled)
						logger.info("No more results.");	
					break;
				}
				
				if(loggingEnabled) {
					logger.info(statuses.size() + " statuses retrieved.");	
				}
				
				for(Status status : statuses) {
					if(status != null) {
						
						if(sinceDate != null) {
							Date createdAt = status.getCreatedAt();
							if(newSinceDate.before(createdAt)) {
								newSinceDate = new Date(createdAt.getTime());
							}
							if(sinceDate.after(createdAt)) {
								sinceDateReached = true;
								break;
							}
						}
						
						Item twitterItem = new TwitterItem(status);
						if(label != null) {
							twitterItem.addLabel(label);
						}
						
						items.add(twitterItem);
					}
				}
				
				if(numberOfRequests >= requests) {
					if(loggingEnabled) {
						logger.info("numberOfRequests: " + numberOfRequests + " > " + requests);
					}
					break;
				}
				
				if(sinceDateReached) {
					if(loggingEnabled) {
						logger.info("Since date reached: " + sinceDate);
					}
					break;
				}
				
				query = queryResult.nextQuery();
				if(query == null) {
					logger.info("Next Query is null");
					break;
				}
				
				if(loggingEnabled) {
					logger.info("Request for " + query);
				}
				
				queryResult = twitter.search(query);
			}
			
		} catch (Exception e) {
			logger.error(e.getMessage());
		}	
	
		response.setItems(items);
		response.setRequests(numberOfRequests);
		
		return response;
	}
	
	@Override
	public Response retrieveLocationFeed(LocationFeed feed, Integer requests) {
		
		Response response = new Response();
		List<Item> items = new ArrayList<Item>();
		
		int count = 100;
		
		Integer numberOfRequests = 0;
		Date sinceDate = new Date(feed.getSinceDate());
		
		String label = feed.getLabel();
		
		Location location = feed.getLocation();
		if(location == null)
			return response;
		
		//Set the query
		Query query = new Query();
		Double radius = location.getRadius();
		if(radius == null) {
			radius = 1.5; // default radius 1.5 Km 
		}
		
		GeoLocation geoLocation = new GeoLocation(location.getLatitude(), location.getLongitude());
		query.setGeoCode(geoLocation, radius, Query.KILOMETERS);
		query.count(count);
				
		boolean sinceDateReached = false;
		while(true) {
			try {
				numberOfRequests++;
				QueryResult queryResult = twitter.search(query);
				List<Status> statuses = queryResult.getTweets();
				
				for(Status status : statuses) {
					if(status != null) {
						
						if(sinceDate != null) {
							Date createdAt = status.getCreatedAt();
							if(sinceDate.after(createdAt)) {
								sinceDateReached = true;
								break;
							}
						}
						
						Item twitterItem = new TwitterItem(status);
						if(label != null) {
							twitterItem.addLabel(label);
						}
						
						items.add(twitterItem);
					}
				}
				
				if(!queryResult.hasNext()) {
					if(loggingEnabled)
						logger.info("There is not next query.");
					break;
				}
				
				if(numberOfRequests > requests) {
					if(loggingEnabled)
						logger.info("numberOfRequests: " + numberOfRequests + " > " + requests);
					break;
				}
				if(sinceDateReached) {
					if(loggingEnabled)
						logger.info("Since date reached: " + sinceDate);
					break;
				}
				
				query = queryResult.nextQuery();
				if(query == null)
					break;
			} catch (TwitterException e) {
				logger.error(e);
				break;
			}
		}
		
		response.setItems(items);
		response.setRequests(numberOfRequests);
		
		return response;
	}
	
	@Override
	public Response retrieveGroupFeed(GroupFeed feed, Integer requests) {
		
		Response response = new Response();
		List<Item> items = new ArrayList<Item>();
		
		Integer numberOfRequests = 0;

		String label = feed.getLabel();
			
		String ownerScreenName = feed.getGroupCreator();
		String slug = feed.getGroupId();
				
		int page = 1;
		Paging paging = new Paging(page, 200);
		while(true) {
			try {
				numberOfRequests++;
				ResponseList<Status> responseList = twitter.getUserListStatuses(ownerScreenName, slug, paging);
				for(Status status : responseList) {
					if(status != null) {
						Item twitterItem = new TwitterItem(status);
						if(label != null) {
							twitterItem.addLabel(label);
						}
						
						items.add(twitterItem);
					}
				}
					
				if(numberOfRequests > requests) {
					if(loggingEnabled)
						logger.info("numberOfRequests: " + numberOfRequests + " > " + requests);
					break;
				}
				
				paging.setPage(++page);
			} catch (TwitterException e) {
				logger.error(e);	
				break;
			}
		}
		
		response.setItems(items);
		response.setRequests(numberOfRequests);
		
		return response;
	}
	
	
	@Override
	public void stop() {
		twitter = null;
	}

	@Override
	public MediaItem getMediaItem(String id) {
		return null;
	}

	@Override
	public StreamUser getStreamUser(String uid) {
		try {
			long userId = Long.parseLong(uid);
			User user = twitter.showUser(userId);
			
			StreamUser streamUser = new TwitterStreamUser(user);
			return streamUser;
		}
		catch(Exception e) {
			logger.error(e);
			return null;
		}
	}


	public static void main(String...args) throws Exception {
		
		Credentials credentials = new Credentials ();
		credentials.setKey("");
		credentials.setSecret("");
		credentials.setAccessToken("");
		credentials.setAccessTokenSecret("");
		
		TwitterRetriever retriever = new TwitterRetriever(credentials);
	
		Date since = new Date(System.currentTimeMillis() - 30l*24l*3600000l);
		
		List<String> keywords = new ArrayList<String>();
		keywords.add("(bbc AND bias)");
		keywords.add("(bbc AND impartial)");
		keywords.add("(bbc AND partisan)");
		keywords.add("(bbc AND left AND wing)");
		keywords.add("(bbc AND right AND wing)");
		
		KeywordsFeed feed = new KeywordsFeed("1", keywords, since.getTime(), "Twitter");
		
		Response response = retriever.retrieve(feed, 10);
		for(Item item : response.getItems()) {
			System.out.println(item.getTitle().replaceAll("\n", " "));
			System.out.println(new Date(item.getPublicationTime()));
			System.out.println("From: " + item.getStreamUser().getUsername());
			System.out.println("==============================================");
		}
		
		retriever.printAvailableReq();
		
	}
	
	private void printAvailableReq() {
		try {
			Map<String, RateLimitStatus> rateLimits = twitter.getRateLimitStatus();
			for(Entry<String, RateLimitStatus> e : rateLimits.entrySet()) {
				System.out.println(e.getKey());
				System.out.println(e.getValue());
				System.out.println("===================================");
			}
		} catch (TwitterException e) {
			e.printStackTrace();
		}
		
	}
	
}
