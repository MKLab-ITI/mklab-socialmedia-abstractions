package gr.iti.mklab.framework.retrievers.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

import twitter4j.GeoLocation;
import twitter4j.Paging;
import twitter4j.Query;
import twitter4j.QueryResult;
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
import gr.iti.mklab.framework.common.domain.Feed;
import gr.iti.mklab.framework.common.domain.Item;
import gr.iti.mklab.framework.common.domain.Keyword;
import gr.iti.mklab.framework.common.domain.Location;
import gr.iti.mklab.framework.common.domain.MediaItem;
import gr.iti.mklab.framework.common.domain.Source;
import gr.iti.mklab.framework.common.domain.StreamUser;
import gr.iti.mklab.framework.common.domain.feeds.KeywordsFeed;
import gr.iti.mklab.framework.common.domain.feeds.ListFeed;
import gr.iti.mklab.framework.common.domain.feeds.LocationFeed;
import gr.iti.mklab.framework.common.domain.feeds.SourceFeed;

/**
 * Class responsible for retrieving Twitter content based on keywords, twitter users or locations
 * The retrieval process takes place through Twitter API (twitter4j)
 * @author ailiakop
 * @email  ailiakop@iti.gr
 */
public class TwitterRetriever extends SocialMediaRetriever {
	
	private Logger  logger = Logger.getLogger(TwitterRetriever.class);
	private boolean loggingEnabled = false;
	
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
		
		this.tf = new TwitterFactory(conf);
		twitter = tf.getInstance();
	
	}
	
	@Override
	public List<Item> retrieveUserFeeds(SourceFeed feed, Integer maxRequests, Integer maxResults) {
		
		List<Item> items = new ArrayList<Item>();
		
		int count = 200;
		
		Integer numberOfRequests = 0;
		
		Source source = feed.getSource();
		if(source == null)
			return items;
		
		Date sinceDate = feed.getDateToRetrieve();
		Date newSinceDate = sinceDate;
		
		String label = feed.getLabel();
		
		String userId = source.getId();
		String screenName = source.getName();
		
		int page = 1;
		Paging paging = new Paging(page, count);
		boolean sinceDateReached = false;
		while(true) {
			try {
				ResponseList<Status> response = null;
				if(userId != null) {
					response = twitter.getUserTimeline(Integer.parseInt(userId), paging);
				}
				else if(screenName != null) {
					if(loggingEnabled)
						logger.info("Retrieve timeline for " + screenName + ". Page: " + paging);
					response = twitter.getUserTimeline(screenName, paging);
				}
				else {
					break;
				}
				numberOfRequests++;
				
				for(Status status : response) {
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
						
						TwitterItem twitterItem = new TwitterItem(status);
						twitterItem.setList(label);
						
						items.add(twitterItem);
					}
				}
				
				if(items.size() > maxResults) {
					if(loggingEnabled)
						if(loggingEnabled)logger.info("totalRetrievedItems: " + items.size() + " > " + maxResults);
					break;
				}
				if(numberOfRequests >= maxRequests) {
					if(loggingEnabled)	
						if(loggingEnabled)logger.info("numberOfRequests: " + numberOfRequests + " > " + maxRequests);
					break;
				}
				if(sinceDateReached) {
					if(loggingEnabled)
						if(loggingEnabled)logger.info("Since date reached: " + sinceDate);
					break;
				}
				
				paging.setPage(++page);
			} catch (TwitterException e) {
				logger.error(e);
				break;
			}
		}
		feed.setDateToRetrieve(newSinceDate);
		return items;
		
	}
	
	@Override
	public List<Item> retrieveKeywordsFeeds(KeywordsFeed feed, Integer maxRequests, Integer maxResults) {
			
		
		List<Item> items = new ArrayList<Item>();
		
		int count = 100;
		int numberOfRequests = 0;

		Date sinceDate = feed.getDateToRetrieve();
		Date newSinceDate = sinceDate;
		
		String label = feed.getLabel();
		
		Keyword keyword = feed.getKeyword();
		List<Keyword> keywords = feed.getKeywords();
		
		if(keywords == null && keyword == null) {
			logger.error("#Twitter : No keywords feed");
			return items;
		}
		
		
		String tags = "";
		
		if(keyword != null) {
			tags += keyword.getName().toLowerCase();
			tags = tags.trim();
		}
		else if(keywords != null){
			for(Keyword key : keywords){
				String [] words = key.getName().split(" ");
				for(String word : words)
					if(!tags.contains(word) && word.length()>1)
						tags += word.toLowerCase()+" ";
			}
		}
		
		if(tags.equals("")) 
			return items;
		
		//Set the query
		if(loggingEnabled)
			logger.info("Query String: " + tags);
		Query query = new Query(tags);
	
		query.count(count);
		query.setResultType(Query.RECENT); //do not set last item date-causes problems!

		boolean sinceDateReached = false;
		try {
			if(loggingEnabled)
				logger.info("Request for " + query);
			
			QueryResult response = twitter.search(query);
			while(response != null) {
				numberOfRequests++;
				
				List<Status> statuses = response.getTweets();
				
				if(statuses == null || statuses.isEmpty()) {
					if(loggingEnabled)
						logger.info("No more results.");	
					break;
				}
				
				if(loggingEnabled)
					logger.info(statuses.size() + " statuses retrieved.");	
				
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
						
						TwitterItem twitterItem = new TwitterItem(status);
						twitterItem.setList(label);
						
						items.add(twitterItem);
					}
				}
				
				if(items.size() > maxResults) {
					if(loggingEnabled)
						logger.info("totalRetrievedItems: " + items.size() + " > " + maxResults);
					break;
				}
				if(numberOfRequests >= maxRequests) {
					if(loggingEnabled)
						logger.info("numberOfRequests: " + numberOfRequests + " > " + maxRequests);
					break;
				}
				if(sinceDateReached) {
					if(loggingEnabled)
						logger.info("Since date reached: " + sinceDate);
					break;
				}
			
				query = response.nextQuery();
				if(query == null)
					break;
				
				if(loggingEnabled)
					logger.info("Request for " + query);
				response = twitter.search(query);
			}
			
		} catch (Exception e) {
			logger.error(e.getMessage());
		}	
	
		feed.setDateToRetrieve(newSinceDate);
		return items;
	}
	
	@Override
	public List<Item> retrieveLocationFeeds(LocationFeed feed, Integer maxRequests, Integer maxResults) {
		
		List<Item> items = new ArrayList<Item>();
		
		int count = 100;
		
		Integer numberOfRequests = 0;
		Date sinceDate = feed.getDateToRetrieve();
		
		Location location = feed.getLocation();
		if(location == null)
			return items;
		
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
				QueryResult response = twitter.search(query);
				
				
				List<Status> statuses = response.getTweets();
				for(Status status : statuses) {
					if(status != null) {
						
						if(sinceDate != null) {
							Date createdAt = status.getCreatedAt();
							if(sinceDate.after(createdAt)) {
								sinceDateReached = true;
								break;
							}
						}
						
						TwitterItem twitterItem = new TwitterItem(status);
						
						items.add(twitterItem);
					}
				}
				
				if(!response.hasNext()) {
					if(loggingEnabled)
						logger.info("There is not next query.");
					break;
				}
				if(items.size() > maxResults) {
					if(loggingEnabled)
						logger.info("totalRetrievedItems: " + items.size() + " > " + maxResults);
					break;
				}
				if(numberOfRequests > maxRequests) {
					if(loggingEnabled)
						logger.info("numberOfRequests: " + numberOfRequests + " > " + maxRequests);
					break;
				}
				if(sinceDateReached) {
					if(loggingEnabled)
						logger.info("Since date reached: " + sinceDate);
					break;
				}
				
				query = response.nextQuery();
				if(query == null)
					break;
			} catch (TwitterException e) {
				logger.error(e);
				break;
			}
		}
		
		return items;
	}
	
	@Override
	public List<Item> retrieveListsFeeds(ListFeed feed, Integer maxRequests, Integer maxResults) {
		
		List<Item> items = new ArrayList<Item>();
		
		Integer numberOfRequests = 0;

		String label = feed.getLabel();
			
		String ownerScreenName = feed.getListOwner();
		String slug = feed.getListSlug();
				
		int page = 1;
		Paging paging = new Paging(page, 200);
		while(true) {
			try {
				numberOfRequests++;
				ResponseList<Status> response = twitter.getUserListStatuses(ownerScreenName, slug, paging);
				for(Status status : response) {
					if(status != null) {
						TwitterItem twitterItem = new TwitterItem(status);
						twitterItem.setList(label);
						
						items.add(twitterItem);
					}
				}
					
				paging.setPage(++page);
			} catch (TwitterException e) {
				logger.error(e);	
				break;
			}
		}
		return items;
	}
	
	@Override
	public List<Item> retrieve(Feed feed,  Integer maxRequests, Integer maxResults) {
		
		switch(feed.getFeedtype()) {
			case SOURCE:
				SourceFeed userFeed = (SourceFeed) feed;
				if(!userFeed.getSource().getNetwork().equals("Twitter"))
					return new ArrayList<Item>();
				
				return retrieveUserFeeds(userFeed, maxRequests, maxResults);
				
			case KEYWORDS:
				KeywordsFeed keyFeed = (KeywordsFeed) feed;
				return retrieveKeywordsFeeds(keyFeed, maxRequests, maxResults);
			
			case LOCATION:
				LocationFeed locationFeed = (LocationFeed) feed;	
				return retrieveLocationFeeds(locationFeed, maxRequests, maxResults);
			
			case LIST:
				ListFeed listFeed = (ListFeed) feed;
				return retrieveListsFeeds(listFeed, maxRequests, maxResults);
				
			default:
				logger.error("Unkonwn Feed Type: " + feed.toJSONString());
				return new ArrayList<Item>();
		}
	}
	
	
	@Override
	public void stop() {
		if(twitter != null) {
			twitter.shutdown();
		}
		
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


	
}
