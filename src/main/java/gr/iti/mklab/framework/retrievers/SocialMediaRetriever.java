package gr.iti.mklab.framework.retrievers;

import java.util.ArrayList;
import java.util.List;

import gr.iti.mklab.framework.Credentials;
import gr.iti.mklab.framework.common.domain.Item;
import gr.iti.mklab.framework.common.domain.MediaItem;
import gr.iti.mklab.framework.common.domain.StreamUser;
import gr.iti.mklab.framework.common.domain.feeds.AccountFeed;
import gr.iti.mklab.framework.common.domain.feeds.Feed;
import gr.iti.mklab.framework.common.domain.feeds.GroupFeed;
import gr.iti.mklab.framework.common.domain.feeds.KeywordsFeed;
import gr.iti.mklab.framework.common.domain.feeds.LocationFeed;

/**
 * The interface for retrieving from social media - Currently the social networks supprorted by the platform are the following:
 * YouTube, Google+,Twitter, Facebook, Flickr, Instagram, Topsy, Tumblr, Vimeo, DailyMotion, Twitpic
 * 
 * @author Manos Schinas
 * @email  manosetro@iti.gr
 */
public abstract class SocialMediaRetriever implements Retriever {
	
	public SocialMediaRetriever(Credentials credentials) {
		
	}
	
	/**
	 * Retrieves a keywords feed that contains certain keywords
	 * in order to retrieve relevant content
	 * 
	 * @param feed
	 * @return List<Item>
	 * @throws Exception
	 */
	public List<Item> retrieveKeywordsFeed(KeywordsFeed feed) throws Exception {
		return retrieveKeywordsFeed(feed, 1);
	}
	
	public abstract List<Item> retrieveKeywordsFeed(KeywordsFeed feed, Integer requests) throws Exception;
	
	/**
	 * Retrieves a user feed that contains the user/users in 
	 * order to retrieve content posted by them
	 * 
	 * @param feed
	 * @return List<Item>
	 * @throws Exception
	 */
	public List<Item> retrieveAccountFeed(AccountFeed feed) throws Exception {
		return retrieveAccountFeed(feed, 1);
	}
	
	public abstract List<Item> retrieveAccountFeed(AccountFeed feed, Integer requests) throws Exception;
	
	/**
	 * Retrieves a location feed that contains the coordinates of the location
	 * that the retrieved content must come from.
	 * 
	 * @param feed
	 * @return List<Item>
	 * @throws Exception
	 */
	public List<Item> retrieveLocationFeed(LocationFeed feed) throws Exception {
		return retrieveLocationFeed(feed, 1);
	}
	
	public abstract List<Item> retrieveLocationFeed(LocationFeed feed, Integer requests) throws Exception;

	/**
	 * Retrieves a list feed that contains the owner of a list an a slug 
	 * used for the description of the list.
	 * @param feed
	 * @return
	 * @throws Exception
	 */
	public List<Item> retrieveGroupFeed(GroupFeed feed) {
		return retrieveGroupFeed(feed, 1);
	}
	
	public abstract List<Item> retrieveGroupFeed(GroupFeed feed, Integer maxRequests);
	
	/**
	 * Retrieves the info for a specific user on the basis
	 * of his id in the social network
	 * @param uid
	 * @return a StreamUser instance
	 */
	public abstract StreamUser getStreamUser(String uid);
	
	/**
	 * Retrieves the info for a specific media object on the basis of its id in the social network
	 * 
	 * @param id
	 * @return a MediaItem instance
	 */
	public abstract MediaItem getMediaItem(String id);
	
	@Override
	public List<Item> retrieve(Feed feed) throws Exception {
		return retrieve(feed, 1);
	}
	
	@Override
	public List<Item> retrieve (Feed feed, Integer requests) throws Exception {
		if(AccountFeed.class.isInstance(feed)) {
			AccountFeed userFeed = (AccountFeed) feed;				
			return retrieveAccountFeed(userFeed, requests);
		}
		if(KeywordsFeed.class.isInstance(feed)) {
			KeywordsFeed keyFeed = (KeywordsFeed) feed;				
			return retrieveKeywordsFeed(keyFeed, requests);
		}
		if(LocationFeed.class.isInstance(feed)) {
			LocationFeed locationFeed = (LocationFeed) feed;
			return retrieveLocationFeed(locationFeed, requests);
		}
		if(GroupFeed.class.isInstance(feed)) {
			GroupFeed listFeed = (GroupFeed) feed;
			return retrieveGroupFeed(listFeed, requests);
		}
		return new ArrayList<Item>();
	}
	
}
