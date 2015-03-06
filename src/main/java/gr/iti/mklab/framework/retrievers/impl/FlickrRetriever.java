package gr.iti.mklab.framework.retrievers.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.flickr4java.flickr.Flickr;
import com.flickr4java.flickr.FlickrException;
import com.flickr4java.flickr.REST;
import com.flickr4java.flickr.people.PeopleInterface;
import com.flickr4java.flickr.people.User;
import com.flickr4java.flickr.photos.Extras;
import com.flickr4java.flickr.photos.Photo;
import com.flickr4java.flickr.photos.PhotoList;
import com.flickr4java.flickr.photos.PhotosInterface;
import com.flickr4java.flickr.photos.SearchParameters;

import gr.iti.mklab.framework.Credentials;
import gr.iti.mklab.framework.abstractions.socialmedia.items.FlickrItem;
import gr.iti.mklab.framework.abstractions.socialmedia.users.FlickrStreamUser;
import gr.iti.mklab.framework.common.domain.Item;
import gr.iti.mklab.framework.common.domain.MediaItem;
import gr.iti.mklab.framework.common.domain.StreamUser;
import gr.iti.mklab.framework.common.domain.feeds.AccountFeed;
import gr.iti.mklab.framework.common.domain.feeds.Feed;
import gr.iti.mklab.framework.common.domain.feeds.GroupFeed;
import gr.iti.mklab.framework.common.domain.feeds.KeywordsFeed;
import gr.iti.mklab.framework.common.domain.feeds.LocationFeed;
import gr.iti.mklab.framework.retrievers.SocialMediaRetriever;

/**
 * Class responsible for retrieving Flickr content based on keywords,users or location coordinates
 * The retrieval process takes place through Flickr API.
 * 
 * @author manosetro
 * @email  manosetro@iti.gr
 */
public class FlickrRetriever extends SocialMediaRetriever {

	private Logger logger = Logger.getLogger(FlickrRetriever.class);
	
	private static final int PER_PAGE = 500;
	
	private String flickrKey;
	private String flickrSecret;

	private Flickr flickr;

	private HashMap<String, StreamUser> userMap;
	

	public FlickrRetriever(Credentials credentials) {
		super(credentials);
		
		this.flickrKey = credentials.getKey();
		this.flickrSecret = credentials.getSecret();
		
		userMap = new HashMap<String, StreamUser>();
		
		Flickr.debugStream = false;
		
		this.flickr = new Flickr(flickrKey, flickrSecret, new REST());
	}
	
	@Override
	public List<Item> retrieveAccountFeed(AccountFeed feed, Integer maxResults, Integer maxRequests) {
		
		List<Item> items = new ArrayList<Item>();
		
		Date dateToRetrieve = feed.getSinceDate();
		String label = feed.getLabel();
		
		int page=1, pages=1; //pagination
		int numberOfRequests = 0;
		int numberOfResults = 0;
		
		String userID = feed.getId();
		if(userID == null) {
			logger.info("#Flickr : No source feed");
			return items;
		}
		
		PhotosInterface photosInteface = flickr.getPhotosInterface();
		SearchParameters params = new SearchParameters();
		params.setUserId(userID);
		params.setMinUploadDate(dateToRetrieve);
		
		Set<String> extras = new HashSet<String>(Extras.ALL_EXTRAS);
		extras.remove(Extras.MACHINE_TAGS);
		params.setExtras(extras);
		
		while(page<=pages && numberOfRequests<=maxRequests && numberOfResults<=maxResults) {
			
			PhotoList<Photo> photos;
			try {
				numberOfRequests++;
				photos = photosInteface.search(params , PER_PAGE, page++);
			} catch (Exception e) {
				break;
			}
			
			pages = photos.getPages();
			numberOfResults += photos.size();

			if(photos.isEmpty()) {
				break;
			}
		
			for(Photo photo : photos) {

				String userid = photo.getOwner().getId();
				StreamUser streamUser = userMap.get(userid);
				if(streamUser == null) {
					streamUser = getStreamUser(userid);
					userMap.put(userid, streamUser);
				}

				FlickrItem flickrItem = new FlickrItem(photo, streamUser);
				if(label != null) {
					flickrItem.addLabel(label);
				}
				
				items.add(flickrItem);
			}
		}
		
		//logger.info("#Flickr : Done retrieving for this session");
//		logger.info("#Flickr : Handler fetched " + items.size() + " photos from " + userID + 
//				" [ " + lastItemDate + " - " + new Date(System.currentTimeMillis()) + " ]");
		
		// The next request will retrieve only items of the last day
		dateToRetrieve = new Date(System.currentTimeMillis() - (24*3600*1000));
		feed.setSinceDate(dateToRetrieve);
		
		return items;
	}
	
	@Override
	public List<Item> retrieveKeywordsFeed(KeywordsFeed feed, Integer maxResults, Integer maxRequests) {
		
		List<Item> items = new ArrayList<Item>();
		
		Date dateToRetrieve = feed.getSinceDate();
		String label = feed.getLabel();
		
		int page=1, pages=1;
		
		int numberOfRequests = 0;
		int numberOfResults = 0;
		
		List<String> keywords = feed.getKeywords();
		
		if(keywords == null || keywords.isEmpty()) {
			logger.error("#Flickr : Text is emtpy");
			return items;
		}
		
		List<String> tags = new ArrayList<String>();
		String text = "";
		for(String key : keywords) {
			String [] words = key.split("\\s+");
			for(String word : words) {
				if(!tags.contains(word) && word.length()>1) {
					tags.add(word);
					text += (word + " ");
				}
			}
		}
		
		
		if(text.equals("")) {
			logger.error("#Flickr : Text is emtpy");
			return items;
		}
		
		PhotosInterface photosInteface = flickr.getPhotosInterface();
		SearchParameters params = new SearchParameters();
		params.setText(text);
		params.setMinUploadDate(dateToRetrieve);
		
		Set<String> extras = new HashSet<String>(Extras.ALL_EXTRAS);
		extras.remove(Extras.MACHINE_TAGS);
		params.setExtras(extras);
		
		while(page<=pages && numberOfRequests<=maxRequests && numberOfResults<=maxResults ) {
			
			PhotoList<Photo> photos;
			try {
				numberOfRequests++;
				photos = photosInteface.search(params , PER_PAGE, page++);
			} catch (Exception e) {
				logger.error("Exception: " + e.getMessage());
				continue;
			}
			
			pages = photos.getPages();
			numberOfResults += photos.size();

			if(photos.isEmpty()) {
				break;
			}
		
			for(Photo photo : photos) {

				String userid = photo.getOwner().getId();
				StreamUser streamUser = userMap.get(userid);
				if(streamUser == null) {
					streamUser = getStreamUser(userid);
					userMap.put(userid, streamUser);
				}

				Item flickrItem = new FlickrItem(photo, streamUser);
				if(label != null) {
					flickrItem.addLabel(label);
				}
				
				items.add(flickrItem);
			}
		}
			
//		logger.info("#Flickr : Done retrieving for this session");
//		logger.info("#Flickr : Handler fetched " + items.size() + " photos from " + text + 
//				" [ " + lastItemDate + " - " + new Date(System.currentTimeMillis()) + " ]");
		
		dateToRetrieve = new Date(System.currentTimeMillis() - (24*3600*1000));
		feed.setSinceDate(dateToRetrieve);
		
		return items;
	}
	
	@Override
	public List<Item> retrieveLocationFeed(LocationFeed feed, Integer maxResults, Integer maxRequests){
		
		List<Item> items = new ArrayList<Item>();
		
		Date dateToRetrieve = feed.getSinceDate();
		String label = feed.getLabel();
		
		Double[][] bbox = feed.getLocation().getbbox();
		
		if(bbox == null || bbox.length==0)
			return items;
		
		int page=1, pages=1;
		int numberOfRequests = 0;
		int numberOfResults = 0;
		
		PhotosInterface photosInteface = flickr.getPhotosInterface();
		SearchParameters params = new SearchParameters();
		params.setBBox(bbox[0][0].toString(), bbox[0][1].toString(), bbox[1][0].toString(), bbox[1][1].toString());
		params.setMinUploadDate(dateToRetrieve);
		
		Set<String> extras = new HashSet<String>(Extras.ALL_EXTRAS);
		extras.remove(Extras.MACHINE_TAGS);
		params.setExtras(extras);
		
		while(page<=pages && numberOfRequests<=maxRequests && numberOfResults<=maxResults ) {
			
			PhotoList<Photo> photos;
			try {
				photos = photosInteface.search(params , PER_PAGE, page++);
			} catch (FlickrException e) {
				break;
			}
			
			pages = photos.getPages();
			numberOfResults += photos.size();

			if(photos.isEmpty()) {
				break;
			}
		
			for(Photo photo : photos) {

				String userid = photo.getOwner().getId();
				StreamUser streamUser = userMap.get(userid);
				if(streamUser == null) {
					streamUser = getStreamUser(userid);

					userMap.put(userid, streamUser);
				}

				Item flickrItem = new FlickrItem(photo, streamUser);
				if(label != null) {
					flickrItem.addLabel(label);
				}
				
				items.add(flickrItem);
			}
		}
		
		logger.info("#Flickr : Handler fetched " + items.size() + " photos "+ 
				" [ " + dateToRetrieve + " - " + new Date(System.currentTimeMillis()) + " ]");
		
		return items;
    }
	
	@Override
	public List<Item> retrieveGroupFeed(GroupFeed feed, Integer maxRequests, Integer maxResults) {
		return null;
	}
	
	@Override
	public void stop(){
		if(flickr != null)
			flickr = null;
	}
	
	@Override
	public MediaItem getMediaItem(String id) {
		return null;
	}
	
	@Override
	public StreamUser getStreamUser(String uid) {
		try {
			PeopleInterface peopleInterface = flickr.getPeopleInterface();
			User user = peopleInterface.getInfo(uid);
			
			StreamUser streamUser = new FlickrStreamUser(user);
			return streamUser;
		}
		catch(Exception e) {
			return null;
		}
		
	}
	
	public static void main(String...args) throws Exception {
		
		String flickrKey = "029eab4d06c40e08670d78055bf61205";
		String flickrSecret = "bc4105126a4dfb8c";
		
		Credentials credentials = new Credentials();
		credentials.setKey(flickrKey);
		credentials.setSecret(flickrSecret);
		
		FlickrRetriever retriever = new FlickrRetriever(credentials);
		
		Feed feed = new KeywordsFeed( "1", "\"uk\" amazing", new Date(System.currentTimeMillis()-14400000));
		
		List<Item> items = retriever.retrieve(feed, 1, 1000);
		System.out.println(items.size());
	}
	
}
