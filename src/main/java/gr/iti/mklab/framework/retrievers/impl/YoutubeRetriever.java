package gr.iti.mklab.framework.retrievers.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.api.services.youtube.YouTube;
import com.google.api.services.youtube.model.Channel;
import com.google.api.services.youtube.model.ChannelListResponse;
import com.google.api.services.youtube.model.SearchListResponse;
import com.google.api.services.youtube.model.SearchResult;
import com.google.api.services.youtube.model.Video;
import com.google.api.services.youtube.model.VideoListResponse;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.Joiner;

import gr.iti.mklab.framework.Credentials;
import gr.iti.mklab.framework.abstractions.socialmedia.items.YoutubeItem;
import gr.iti.mklab.framework.abstractions.socialmedia.users.YoutubeStreamUser;
import gr.iti.mklab.framework.common.domain.Item;
import gr.iti.mklab.framework.common.domain.MediaItem;
import gr.iti.mklab.framework.common.domain.StreamUser;
import gr.iti.mklab.framework.common.domain.feeds.AccountFeed;
import gr.iti.mklab.framework.common.domain.feeds.GroupFeed;
import gr.iti.mklab.framework.common.domain.feeds.KeywordsFeed;
import gr.iti.mklab.framework.common.domain.feeds.LocationFeed;
import gr.iti.mklab.framework.retrievers.Response;
import gr.iti.mklab.framework.retrievers.SocialMediaRetriever;

public class YoutubeRetriever extends SocialMediaRetriever{

	private Logger  logger = LogManager.getLogger(YoutubeRetriever.class);

	private String apiKey;
	
	private static YouTube youtubeService;
	
	public static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
	public static final JsonFactory JSON_FACTORY = new JacksonFactory();
	
	private static final long NUMBER_OF_VIDEOS_RETURNED = 50;
	
	public YoutubeRetriever(Credentials credentials) {
		super(credentials);
		
		apiKey = credentials.getKey();
		
		// This object is used to make YouTube Data API requests. The last argument is required, but since we don't need anything
        // initialized when the HttpRequest is initialized, we override the interface and provide a no-op function.
		youtubeService = new YouTube.Builder(HTTP_TRANSPORT, JSON_FACTORY, new HttpRequestInitializer() {
            public void initialize(HttpRequest request) throws IOException {
            }
        }).setApplicationName("youtube-search-module").build();
		
	}

	@Override
	public void stop() {
		
	}

	@Override
	public Response retrieveKeywordsFeed(KeywordsFeed feed, Integer requests) throws Exception {
				
		String label = feed.getLabel();
		long sinceDate = feed.getSinceDate();
		
		List<Item> items = new ArrayList<Item>();
		int numberOfRequests = 0;
		
		// Define the API request for retrieving search results.
        YouTube.Search.List search = youtubeService.search().list("id");
        search.setKey(apiKey);
        		
        List<String> keywords = feed.getKeywords();
		if(keywords == null || keywords.isEmpty()) {
			logger.error("#Youtube : No keywords feed");
			Response response = getResponse(items, numberOfRequests);
			return response;
		}
		
		String textQuery = StringUtils.join(keywords, " OR ");
		if(textQuery.equals("")) {
			logger.error("Text Query is empty.");
			Response response = getResponse(items, numberOfRequests);
			return response;
		}
        search.setQ(textQuery);
        search.setType("video");
        search.setMaxResults(NUMBER_OF_VIDEOS_RETURNED);
        search.setOrder("date");
        
        Set<String> uids = new HashSet<String>();
        boolean sinceDateReached = false;
        String nextPageToken = null;
        while(true) {
        	try {
        		
        		if(nextPageToken != null) {
        			search.setPageToken(nextPageToken);
        		}
        		
        		SearchListResponse searchResponse = search.execute();
        		numberOfRequests++;
        	
        		List<SearchResult> searchResultList = searchResponse.getItems();
        		if (searchResultList != null) {
 
        			List<String> videoIds = new ArrayList<String>();
        			for (SearchResult searchResult : searchResultList) {
        				videoIds.add(searchResult.getId().getVideoId());
        			}
        			Joiner stringJoiner = Joiner.on(',');
        			String videoId = stringJoiner.join(videoIds);
                
        			YouTube.Videos.List listVideosRequest = youtubeService.videos().list("snippet,recordingDetails,player");
        			listVideosRequest.setId(videoId);
        			listVideosRequest.setKey(apiKey);
        			listVideosRequest.setMaxResults(NUMBER_OF_VIDEOS_RETURNED);
                	VideoListResponse listResponse = listVideosRequest.execute();
                	numberOfRequests++;
                
                	List<Video> videoList = listResponse.getItems();
                	if (videoList != null) {
                		for(Video video : videoList) {
                			uids.add(video.getSnippet().getChannelId());
                			
                			Item item = new YoutubeItem(video);
                			if(item.getPublicationTime() < sinceDate) {
                				System.out.println(new Date(item.getPublicationTime()) + " < " + new Date(sinceDate));
                				sinceDateReached = true;
								break;
                			}
                			
							if(label != null) {
								item.addLabel(label);
							}
						
                			items.add(item);
                		}
                	}
        		}
        	
        		nextPageToken = searchResponse.getNextPageToken();
        		if(nextPageToken == null) {
        			logger.info("Stop retriever. There is no more pages to fetch for query " + textQuery);
        			break;
        		}
        				
			} catch (GoogleJsonResponseException e) {
				logger.error("There was a service error: " + e.getDetails().getCode() + " : " + e.getDetails().getMessage(), e);
				break;
			} catch (IOException e) {
				logger.error("There was an IO error: " + e.getCause() + " : " + e.getMessage(), e);
				break;
			} catch (Throwable t) {
				logger.error(t);
				break;
			}
        
        	if(numberOfRequests >= requests) {
        		logger.info("Stop retriever. Number of requests (" + numberOfRequests + ") has reached for " + textQuery);
				break;
			}
        	
			if(sinceDateReached) {
				logger.info("Stop retriever. Since date " + sinceDate + " reached for query " + textQuery);
				break;
			}
			
        }
        
//        Map<String, StreamUser> users = getStreamUsers(uids);
//        for(Item item : items) {
//        	String uid = item.getUserId();
//        	StreamUser streamUser = users.get(uid);
//        	item.setStreamUser(streamUser);
//        }
        
		Response response = getResponse(items, numberOfRequests);
		return response;
	}

	@Override
	public Response retrieveAccountFeed(AccountFeed feed, Integer requests) throws Exception {

		List<Item> items = new ArrayList<Item>();
		int numberOfRequests = 0;
		
		long sinceDate = feed.getSinceDate();
		String label = feed.getLabel();
		
		String uName = feed.getUsername();
		
		if(uName == null) {
			logger.error("#YouTube : No source feed");
			Response response = getResponse(items, numberOfRequests);
			return response;
		}
				
		StreamUser streamUser = getStreamUserForUsername(uName);
		numberOfRequests++;
		if(streamUser == null) {
			Response response = getResponse(items, numberOfRequests);
			return response;
		}
		logger.info("#YouTube: Retrieving User Feed: " + streamUser.getUserid());
		
		// Define the API request for retrieving search results.
        YouTube.Search.List search = youtubeService.search().list("id");
        search.setKey(apiKey);
        search.setChannelId(streamUser.getUserid());
        search.setMaxResults(NUMBER_OF_VIDEOS_RETURNED);
        
		boolean sinceDateReached = false;
		String nextPageToken = null;
		while(true) {
			try {
				if(nextPageToken != null) {
					search.setPageToken(nextPageToken);
				}
				
				SearchListResponse searchResponse = search.execute();
				numberOfRequests++;
								
				List<SearchResult> searchResultList = searchResponse.getItems();
        		if (searchResultList != null) {
        			List<String> videoIds = new ArrayList<String>();
        			for (SearchResult searchResult : searchResultList) {
        				videoIds.add(searchResult.getId().getVideoId());
        			}
        			Joiner stringJoiner = Joiner.on(',');
        			String videoId = stringJoiner.join(videoIds);
        			
        			YouTube.Videos.List listVideosRequest = youtubeService.videos().list("snippet,recordingDetails,player");
        			listVideosRequest.setId(videoId);
        			listVideosRequest.setKey(apiKey);
        			listVideosRequest.setMaxResults(NUMBER_OF_VIDEOS_RETURNED);
                	VideoListResponse listResponse = listVideosRequest.execute();
                	numberOfRequests++;
                	
                	List<Video> videoList = listResponse.getItems();
                	if (videoList != null) {
                		for(Video video : videoList) {
                			Item item = new YoutubeItem(video, streamUser);
                			
                			if(item.getPublicationTime() < sinceDate) {
                				System.out.println(new Date(item.getPublicationTime()) +"<"+ new Date(sinceDate));
                				sinceDateReached = true;
								break;
                			}
                			
							if(label != null) {
								item.addLabel(label);
							}
                			items.add(item);
                		}
                	}
                	
                	nextPageToken = searchResponse.getNextPageToken();
    				if(nextPageToken == null) {
    					logger.info("Stop retriever. There is no more pages to fetch for " + uName);
            			break;
    				}
        		}
        		else {
        			logger.info("Stop retriever. No more results in response.");
        			break;
        		}
			} catch (GoogleJsonResponseException e) {
				logger.error("There was a service error: " + e.getDetails().getCode() + " : " + e.getDetails().getMessage(), e);
				break;
			} catch (IOException e) {
				logger.error("There was an IO error: " + e.getCause() + " : " + e.getMessage(), e);
				break;
			} catch (Throwable t) {
				logger.error(t);
				break;
			}
		
			if(numberOfRequests >= requests) {
        		logger.info("Stop retriever. Number of requests (" + numberOfRequests + ") has reached for " + uName);
				break;
			}
        	
			if(sinceDateReached) {
				logger.info("Stop retriever. Since date " + sinceDate + " reached for " + uName);
				break;
			}
		}
		
		Response response = getResponse(items, numberOfRequests);
		return response;
	}

	@Override
	public Response retrieveLocationFeed(LocationFeed feed, Integer requests) throws Exception {
		List<Item> items = new ArrayList<Item>();
		int numberOfRequests = 0;
		
		Response response = getResponse(items, numberOfRequests);
		return response;
	}

	@Override
	public Response retrieveGroupFeed(GroupFeed feed, Integer maxRequests) {
		return null;
	}

	public StreamUser getStreamUserForUsername(String uName) {
		
		try {
			YouTube.Channels.List channelListResponse = youtubeService.channels()
					.list("id,snippet,statistics");
			channelListResponse.setKey(apiKey);
			channelListResponse.setForUsername(uName);
			 
			ChannelListResponse response = channelListResponse.execute();
			List<Channel> channels = response.getItems();
			if(channels != null) {
				Channel channel = channels.get(0);
				
				YoutubeStreamUser user = new YoutubeStreamUser(channel);
				user.setUsername(uName);
				
				return user;
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	public StreamUser getStreamUser(String uid) {
		try {
			YouTube.Channels.List channelListResponse = youtubeService.channels()
					.list("id,snippet,statistics");
			
			channelListResponse.setKey(apiKey);
			channelListResponse.setId(uid);
			channelListResponse.setMaxResults(NUMBER_OF_VIDEOS_RETURNED);
			
			ChannelListResponse response = channelListResponse.execute();
			List<Channel> channels = response.getItems();
			if(channels != null) {
				Channel channel = channels.get(0);
				
				YoutubeStreamUser user = new YoutubeStreamUser(channel);

				return user;
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	public Map<String, StreamUser> getStreamUsers(Set<String> uids) {
		Map<String, StreamUser> users = new HashMap<String, StreamUser>();
		try {
			Joiner stringJoiner = Joiner.on(',');
			String userIds = stringJoiner.join(uids);
		        
			YouTube.Channels.List channelListResponse = youtubeService.channels()
					.list("id,snippet,statistics");
			channelListResponse.setKey(apiKey);
			channelListResponse.setId(userIds);
			 
			ChannelListResponse response = channelListResponse.execute();
			List<Channel> channels = response.getItems();
			if(channels != null) {
				for(Channel channel : channels) {
					YoutubeStreamUser user = new YoutubeStreamUser(channel);
					users.put(user.getId(), user);
				}
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}

		return users;
	}
	
	@Override
	public MediaItem getMediaItem(String id) {
		return null;
	}

	public static void main(String...args) throws Exception {
		
		Credentials credentials = new Credentials();
		credentials.setKey("AIzaSyBs4RWhrqw9-3kCvvAN3qKJc79RI2DxOis");
		
		YoutubeRetriever retriever = new YoutubeRetriever(credentials);
		
		long since = System.currentTimeMillis()-(365*24*3600000l);
		KeywordsFeed feed = new KeywordsFeed("id", "barack obama", since, "Youtube");
		
		//AccountFeed feed = new AccountFeed(
		//		"UC16niRr50-MSBwiO3YDb3RA", 
		//		"bbcnews", 
		//		since, 
		//		"Youtube");
		
		Response response = retriever.retrieveKeywordsFeed(feed, 6);
		//Response response = retriever.retrieveAccountFeed(feed, 6);
		for(Item item : response.getItems()) {
			System.out.println(item.getTitle());
		}
		System.out.println(response.getNumberOfItems());
		//StreamUser user = retriever.getStreamUser("bbcnews");
		
		
	}
	
}
