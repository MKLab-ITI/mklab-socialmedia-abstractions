package gr.iti.mklab.framework.retrievers.impl;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.jinstagram.Instagram;
import org.jinstagram.InstagramOembed;
import org.jinstagram.exceptions.InstagramException;
import org.jinstagram.entity.common.Caption;
import org.jinstagram.entity.common.ImageData;
import org.jinstagram.entity.common.Images;
import org.jinstagram.entity.common.Pagination;
import org.jinstagram.entity.common.User;
import org.jinstagram.entity.locations.LocationSearchFeed;
import org.jinstagram.entity.media.MediaInfoFeed;
import org.jinstagram.entity.oembed.OembedInformation;
import org.jinstagram.entity.tags.TagMediaFeed;
import org.jinstagram.entity.users.basicinfo.UserInfo;
import org.jinstagram.entity.users.feed.MediaFeed;
import org.jinstagram.entity.users.feed.MediaFeedData;
import org.jinstagram.entity.users.feed.UserFeed;
import org.jinstagram.entity.users.feed.UserFeedData;
import org.jinstagram.auth.model.Token;

import gr.iti.mklab.framework.Credentials;
import gr.iti.mklab.framework.abstractions.socialmedia.items.InstagramItem;
import gr.iti.mklab.framework.abstractions.socialmedia.users.InstagramStreamUser;
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
 * Class responsible for retrieving Instagram content based on keywords or instagram users or locations
 * The retrieval process takes place through Instagram API.
 * 
 * @author manosetro - manosetro@iti.gr
 */
public class InstagramRetriever extends SocialMediaRetriever {
	
	private Logger logger = LogManager.getLogger(InstagramRetriever.class);
	
	private Instagram instagram = null;

	private InstagramOembed instagramOembed;
	
	public InstagramRetriever(Credentials credentials) {
		
		super(credentials);
		
		Token accessToken = new Token(credentials.getAccessToken(), credentials.getAccessTokenSecret()); 
		this.instagram = new Instagram(credentials.getKey());
		this.instagram.setAccessToken(accessToken);
		this.instagramOembed = new InstagramOembed();
	}
	
	@Override
	public Response retrieveAccountFeed(AccountFeed feed, Integer maxRequests) {
		
		List<Item> items = new ArrayList<Item>();
		
		Date sinceDate = new Date(feed.getSinceDate());
		String label = feed.getLabel();
		
		int numberOfRequests = 0;
		int count = 100;
		
		String uid = feed.getId();
		String uName = feed.getUsername();
		if(uid == null && uName == null) {
			logger.error("#Instagram : No source feed");
			Response response = getResponse(items, numberOfRequests);
			return response;
		}

		StreamUser user = null;
		try {
			if(uid != null) {
				UserInfo userInfo = instagram.getUserInfo(uid);
				user = new InstagramStreamUser(userInfo);
			}
			else {
				UserFeed userf = instagram.searchUser(uName);
				List<UserFeedData> usersFeed = userf.getUserList();
				for(UserFeedData userFeed : usersFeed) {
					if(userFeed.getUserName().equals(uName)) {
						user = new InstagramStreamUser(userFeed);
					}
					
				}
			}	
		}
		catch(InstagramException e) {
			logger.error("#Instagram Exception for feed (" + feed.getId() + ")", e);
			Response response = getResponse(items, numberOfRequests);
			return response;
		}
		
		if(user == null) {
			Response response = getResponse(items, numberOfRequests);
			return response;
		}
		
		try {
			logger.info("#Instagram : Retrieving media feed for user " + user.getId() + " (" + user.getUsername() + ")");
				
			//String userId, int count, String minId, String maxId, Date maxTimeStamp, Date minTimeStamp
			MediaFeed mediaFeed = instagram.getRecentMediaFeed(user.getUserid(), count, null, null, null, sinceDate);
			boolean sinceDateReached = false;
			while(true) {
				if(mediaFeed == null) {
					break;
				}
						
				for(MediaFeedData mfeed : mediaFeed.getData()) {
					int createdTime = Integer.parseInt(mfeed.getCreatedTime());
					Date publicationDate = new Date((long) createdTime * 1000);
							
					if(sinceDate.after(publicationDate)) {
						sinceDateReached = true;
						break;
	    			}
							
					if(mfeed != null && mfeed.getId() != null) {
						Item instagramItem = new InstagramItem(mfeed);
						if(label != null) {
							instagramItem.addLabel(label);
						}
								
						items.add(instagramItem);
					}
				}
						
				Pagination pagination = mediaFeed.getPagination();
				if(pagination == null || !pagination.hasNextPage()) {
					logger.info("Stop retriever. There is no next page for user (" + user.getUsername() + ")");
					break;
				}
						
				if(numberOfRequests >= maxRequests) {
			        logger.info("Stop retriever. Number of requests (" + numberOfRequests + ") has reached for user (" + user.getUsername() + ")");
					break;
				}
			        	
				if(sinceDateReached) {
					logger.info("Stop retriever. Since date " + sinceDate + " reached for user (" + user.getUsername() + ")");
					break;
				}
						
				numberOfRequests++;
				mediaFeed = instagram.getRecentMediaNextPage(pagination);
			}
					
		}
		catch(InstagramException e) {
			logger.error("#Instagram Exception for feed (" + feed.getId() + ")", e);	
			Response response = getResponse(items, numberOfRequests);
			return response;
		} 
		catch (MalformedURLException e) {
			logger.error("#Instagram Exception for (" + feed.getId() + ")", e);
			Response response = getResponse(items, numberOfRequests);
			return response;
		}
		
		Response response = getResponse(items, numberOfRequests);
		return response;
	}
	
	@Override
	public Response retrieveKeywordsFeed(KeywordsFeed feed, Integer maxRequests) {
		
		List<Item> items = new ArrayList<Item>();
		
		Date sinceDate = new Date(feed.getSinceDate());
		String label = feed.getLabel();
		
		int numberOfRequests = 0;

		List<String> keywords = feed.getKeywords();
		if(keywords == null || keywords.isEmpty()) {
			logger.error("#Instagram : No keywords for feed (" + feed.getId() + ")");
			Response response = getResponse(items, numberOfRequests);
			return response;
		}
		
		String tags = "";
		for(String key : keywords) {
			String [] words = key.split(" ");
			for(String word : words) {
				if(!tags.contains(word) && word.length() > 1) {
					tags += word.toLowerCase();
				}
			}
		}
		
		tags = tags.replaceAll(" ", "");
		if(tags.equals("")) {
			logger.error("#Instagram : No keywords feed for query (" + tags + ")");
			Response response = getResponse(items, numberOfRequests);
			return response;
		}
		
		
		boolean sinceDateReached = false;
		TagMediaFeed tagFeed = null;
		try {
			numberOfRequests++;
			tagFeed = instagram.getRecentMediaTags(tags, 50l);
		}
		catch(InstagramException e) {
			logger.error("Instagram retriever exception for (" + feed.getId() + ")", e);
			
			Response response = getResponse(items, numberOfRequests);
			return response;
		}
		
		while(true) {
			try {
				if(tagFeed == null) { 
					logger.info("Stop retriever. Tag feed not found for query (" + tags + ")");
					break;
				}
				
				for(MediaFeedData mediaData : tagFeed.getData()) {
					int createdTime = Integer.parseInt(mediaData.getCreatedTime());
					Date publicationDate = new Date((long) createdTime * 1000);
					
					if(publicationDate.before(sinceDate)) {
						sinceDateReached = true;
						break;
					}

					Item instagramItem = new InstagramItem(mediaData);
					if(label != null) {
						instagramItem.addLabel(label);
					}
					items.add(instagramItem);
				}
				
				Pagination pagination = tagFeed.getPagination();
				
				if(pagination==null || !pagination.hasNextPage()) {
					logger.info("Stop retriever. There is no next page for query (" + tags + ")");
					break;
				}
				
	        	if(numberOfRequests >= maxRequests) {
	        		logger.info("Stop retriever. Number of requests (" + numberOfRequests + ") has reached for query (" + tags + ")");
					break;
				}
	        	
				if(sinceDateReached) {
					logger.info("Stop retriever. Since date " + sinceDate + " reached for query (" + tags + ")");
					break;
				}
				
				numberOfRequests++;
				tagFeed = instagram.getTagMediaInfoNextPage(pagination);
				
			}
			catch(InstagramException e) {
				logger.error("Instagram retriever exception for (" + feed.getId() + ")", e);
				
				Response response = getResponse(items, numberOfRequests);
				return response;
			} catch (MalformedURLException e) {
				logger.error("Instagram retriever exception for (" + feed.getId() + ")", e);
				
				Response response = getResponse(items, numberOfRequests);
				return response;
			}
		}
		
		Response response = getResponse(items, numberOfRequests);
		return response;
	}
	
	@Override
	public Response retrieveLocationFeed(LocationFeed feed, Integer maxRequests) {
		
		List<Item> items = new ArrayList<Item>();
		
		Date lastItemDate = new Date(feed.getSinceDate());
		Date currentDate = new Date(System.currentTimeMillis());
		
		int it = 0 ;
		int numberOfRequests = 0;
		
		boolean isFinished = false;
		
		Location loc = feed.getLocation();
		
    	if(loc == null){ 
    		logger.error("#Instagram : No Location feed");
    		Response response = getResponse(items, numberOfRequests);
    		return response;
    	}
		
		List<org.jinstagram.entity.common.Location> locations = null;
		
    	double latitude = loc.getLatitude();
    	double longtitude = loc.getLongitude();
    	
    	try{
    		LocationSearchFeed locs = instagram.searchLocation(latitude , longtitude,5000);
    		locations = locs.getLocationList();
    	}
    	catch(InstagramException e){
    		logger.error("#Instagram Exception : "+e.getMessage());
    		Response response = getResponse(items, numberOfRequests);
    		return response;
    	}
    	
    	for (org.jinstagram.entity.common.Location location : locations) {
    		
    		Date upDate = currentDate;
    		Date downDate = DateUtils.addDays(upDate, -1); 
    		
    		while(downDate.after(lastItemDate) || downDate.equals(lastItemDate)) {
    	
    			it++;
    			try{
        			MediaFeed mediaFeed = instagram.getRecentMediaByLocation(location.getId(),0,0,upDate,downDate);
        			numberOfRequests++;
        			if(mediaFeed != null) {
        				logger.info("#Instagram : Retrieving page "+it+" that contains "+mediaFeed.getData().size()+" posts");	
            			
                		for(MediaFeedData mfeed : mediaFeed.getData()){
        					int createdTime = Integer.parseInt(mfeed.getCreatedTime());
        					Date publicationDate = new Date((long) createdTime * 1000);
        					if(lastItemDate.after(publicationDate) || numberOfRequests>maxRequests) {
        						isFinished = true;
								break;
        					}
        					
        					if((mfeed != null && mfeed.getId() != null)){
        						InstagramItem instagramItem = new InstagramItem(mfeed);
        						
        						items.add(instagramItem);
        					}
        					
        				}
        			}
        		}
        		catch(InstagramException e){
        			Response response = getResponse(items, numberOfRequests);
        			return response;
        		} catch (MalformedURLException e1) {
        			Response response = getResponse(items, numberOfRequests);
        			return response;
					
				}
    			
    			if(isFinished)
    				break;
    				
    			upDate = downDate;
    			downDate = DateUtils.addDays(upDate, -1); 
    		}
    		
    	}
		
    	Response response = getResponse(items, numberOfRequests);
    	return response;
    }
	
	@Override
	public Response retrieveGroupFeed(GroupFeed feed, Integer maxRequests) {
		return new Response();
	}
	
	@Override
	public void stop() {
		if(instagram != null){
			instagram = null;
		}
	}

	@Override
	public MediaItem getMediaItem(String shortId) {
		try {
			String id = getMediaId("http://instagram.com/p/"+shortId);
			if(id == null)
				return null;
			
			MediaInfoFeed mediaInfo = instagram.getMediaInfo(id);
			if(mediaInfo != null) {
				MediaFeedData mediaData = mediaInfo.getData();
				Images images = mediaData.getImages();
				
				ImageData standardUrl = images.getStandardResolution();
				String url = standardUrl.getImageUrl();
				
				MediaItem mediaItem = new MediaItem(new URL(url));
				
				ImageData thumb = images.getThumbnail();
				String thumbnail = thumb.getImageUrl();
				
				String mediaId = "Instagram#" + mediaData.getId();
				List<String> tags = mediaData.getTags();
				
				String title = null;
				Caption caption = mediaData.getCaption();
				if(caption !=  null) {
					title = caption.getText();
				}
				
				Long publicationTime = new Long(1000*Long.parseLong(mediaData.getCreatedTime()));
				
				//id
				mediaItem.setId(mediaId);
				//SocialNetwork Name
				mediaItem.setSource("Instagram");
				//Reference
				mediaItem.setReference(id);
				//Type 
				mediaItem.setType("image");
				//Time of publication
				mediaItem.setPublicationTime(publicationTime);
				//PageUrl
				mediaItem.setPageUrl(url);
				//Thumbnail
				mediaItem.setThumbnail(thumbnail);
				//Title
				mediaItem.setTitle(title);
				//Tags
				mediaItem.setTags(tags.toArray(new String[tags.size()]));
				//Popularity
				mediaItem.setLikes(new Long(mediaData.getLikes().getCount()));
				mediaItem.setComments(new Long(mediaData.getComments().getCount()));
				//Location
				org.jinstagram.entity.common.Location geoLocation = mediaData.getLocation();
				if(geoLocation != null) {
					double latitude = geoLocation.getLatitude();
					double longitude = geoLocation.getLongitude();
					
					Location location = new Location(latitude, longitude);
					location.setName(geoLocation.getName());
					mediaItem.setLocation(location);
				}
				//Size
				ImageData standard = images.getStandardResolution();
				if(standard!=null) {
					int height = standard.getImageHeight();
					int width = standard.getImageWidth();
					mediaItem.setSize(width, height);
				}
				
				User user = mediaData.getUser();
				if(user != null) {
					StreamUser streamUser = new InstagramStreamUser(user);
					mediaItem.setUser(streamUser);
					mediaItem.setUserId(streamUser.getId());
				}
				
				return mediaItem;
			}
		} catch (Exception e) {
			logger.error(e);
		} 
		
		return null;
	}
	
	@Override
	public StreamUser getStreamUser(String uid) {
		try {
			UserInfo userInfo = instagram.getUserInfo(uid);
			
			StreamUser user = new InstagramStreamUser(userInfo.getData());
			return user;
		}
		catch(Exception e) {
			logger.error("Exception for user " + uid, e);
			return null;
		}
	}
	
	private String getMediaId(String url) {
		try {
			OembedInformation info = instagramOembed.getOembedInformation(url);
			if(info == null) {
				return null;
			}
			return info.getMediaId();
		} catch (Exception e) {
			logger.error("Failed to get id for " + url, e);
		}
		return null;
	}

	public static void main(String...args) throws Exception {
		
		Credentials credentials = new Credentials();
		credentials.setKey("xxxxxxx");
		credentials.setSecret("xxxxxxxx");
		credentials.setAccessToken("xxxxxxxxx");

		InstagramRetriever retriever = new InstagramRetriever(credentials);
		
		long since = System.currentTimeMillis()-(365 * 24 * 3600000l);
		//KeywordsFeed feed = new KeywordsFeed("id", "greek debt", since, "Instagram");
		AccountFeed accountFeed = new AccountFeed(
						"persian.partizan", 
						"persian.partizan", 
						since, 
						"Instagram");
		
		//Response response = retriever.retrieveKeywordsFeed(feed, 3);
		Response response = retriever.retrieveAccountFeed(accountFeed, 3);
		for(Item item : response.getItems()) {
			System.out.println(item.getPageUrl());
		}
		System.out.println(response.getNumberOfItems());
	}
	
}
