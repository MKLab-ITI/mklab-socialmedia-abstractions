package gr.iti.mklab.framework.retrievers.impl;

import java.net.MalformedURLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.scribe.exceptions.OAuthConnectionException;

import com.tumblr.jumblr.JumblrClient;
import com.tumblr.jumblr.exceptions.JumblrException;
import com.tumblr.jumblr.types.Blog;
import com.tumblr.jumblr.types.Post;

import gr.iti.mklab.framework.Credentials;
import gr.iti.mklab.framework.abstractions.socialmedia.items.TumblrItem;
import gr.iti.mklab.framework.abstractions.socialmedia.users.TumblrStreamUser;
import gr.iti.mklab.framework.common.domain.Item;
import gr.iti.mklab.framework.common.domain.MediaItem;
import gr.iti.mklab.framework.common.domain.StreamUser;
import gr.iti.mklab.framework.common.domain.feeds.AccountFeed;
import gr.iti.mklab.framework.common.domain.feeds.GroupFeed;
import gr.iti.mklab.framework.common.domain.feeds.KeywordsFeed;
import gr.iti.mklab.framework.common.domain.feeds.LocationFeed;
import gr.iti.mklab.framework.retrievers.Response;
import gr.iti.mklab.framework.retrievers.Retriever;

/**
 * Class responsible for retrieving Tumblr content based on keywords or tumblr users
 * The retrieval process takes place through Tumblr API (Jumblr).
 * 
 * @author manosetro - manosetro@iti.gr
 */
public class TumblrRetriever extends Retriever {
	
	private Logger logger = LogManager.getLogger(TumblrRetriever.class);
	
	private JumblrClient client;
	
	public TumblrRetriever(Credentials credentials) {
		super(credentials);
		client = new JumblrClient(credentials.getKey(), credentials.getSecret());
	}

	
	@Override
	public Response retrieveAccountFeed(AccountFeed feed, Integer maxRequests) {
		
		Response response = new Response();
		
		List<Item> items = new ArrayList<Item>();
		
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
		Date lastItemDate = new Date(feed.getSinceDate());
		
		int numberOfRequests = 0;
		
		boolean isFinished = false;

		String uName = feed.getUsername();
		
		if(uName == null){
			logger.info("#Tumblr : No source feed");
			return response;
		}
		
		Blog blog = client.blogInfo(uName);
		TumblrStreamUser tumblrStreamUser = new TumblrStreamUser(blog);
		List<Post> posts;
		Map<String,String> options = new HashMap<String,String>();
		
		Integer offset = 0;
		Integer limit = 20;
		options.put("limit", limit.toString());
	
		while(true) {
			
			options.put("offset", offset.toString());
			
			posts = blog.posts(options);
			if(posts == null || posts.isEmpty())
				break;
			
			numberOfRequests++;
			for(Post post : posts) {
				if(post.getType().equals("photo") || post.getType().equals("video") || post.getType().equals("link")) {
					String retrievedDate = post.getDateGMT().replace(" GMT", "") + ".0";
					try {
						Date publicationDate = (Date) formatter.parse(retrievedDate);	
						if(publicationDate.after(lastItemDate) && post != null && post.getId() != null) {
							Item tumblrItem = new TumblrItem(post,tumblrStreamUser);
							items.add(tumblrItem);
						}
						else {
							isFinished = true;
						}
					} catch (Exception e) {
						response.setItems(items);
						response.setRequests(numberOfRequests);
						return response;
					}
				}
			}
			
			if(isFinished || numberOfRequests > maxRequests)
				break;
			
			offset += limit;
		}

		response.setItems(items);
		response.setRequests(numberOfRequests);
		
		return response;
	}
	
	@Override
	public Response retrieveKeywordsFeed(KeywordsFeed feed, Integer maxRequests) {
		
		Response response = new Response();
		List<Item> items = new ArrayList<Item>();
		
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
		Date currentDate = new Date(System.currentTimeMillis());
		Date indexDate = currentDate;
		Date lastItemDate = new Date(feed.getSinceDate());
		DateUtil dateUtil = new DateUtil();
		
		int numberOfRequests=0;
		
		boolean isFinished = false;
		
		List<String> keywords = feed.getKeywords();
		
		if(keywords == null || keywords.isEmpty()) {
			logger.info("#Tumblr : No keywords feed");
			return response;
		}
		
		String tags = "";
		for(String key : keywords) {
			String [] words = key.split("\\s+");
			for(String word : words) {
				if(!tags.contains(word) && word.length()>1) {
					tags += word.toLowerCase()+" ";
				}
			}
		}
		
		
		if(tags.equals(""))
			return response;
		
		while(indexDate.after(lastItemDate) || indexDate.equals(lastItemDate)){
			
			Map<String,String> options = new HashMap<String,String>();
			Long checkTimestamp = indexDate.getTime();
			Integer check = checkTimestamp.intValue();
			options.put("featured_timestamp", check.toString());
			List<Post> posts;
			try{
				posts = client.tagged(tags);
			}catch(JumblrException e){
				response.setItems(items);
				response.setRequests(numberOfRequests);
				return response;
			}catch(OAuthConnectionException e1){
				response.setItems(items);
				response.setRequests(numberOfRequests);
				return response;
			}
			
			if(posts == null || posts.isEmpty())
				break;
			
			numberOfRequests ++;
			
			for(Post post : posts){
				
				if(post.getType().equals("photo") || post.getType().equals("video") ||  post.getType().equals("link")) {
					
					String retrievedDate = post.getDateGMT().replace(" GMT", "");
					retrievedDate+=".0";
					Date publicationDate = null;
					try {
						publicationDate = (Date) formatter.parse(retrievedDate);
						
					} catch (ParseException e) {
						response.setItems(items);
						response.setRequests(numberOfRequests);
						return response;
					}
					
					if(publicationDate.after(lastItemDate) && post != null && post.getId() != null){
						//Get the blog
						String blogName = post.getBlogName();
						Blog blog = client.blogInfo(blogName);
						TumblrStreamUser tumblrStreamUser = new TumblrStreamUser(blog);
						
						TumblrItem tumblrItem = null;
						try {
							tumblrItem = new TumblrItem(post, tumblrStreamUser);
						} catch (MalformedURLException e) {
							response.setItems(items);
							response.setRequests(numberOfRequests);
							return response;
						}
						
						if(tumblrItem != null){
							items.add(tumblrItem);
						}
						
					}
				
				}
				
				if(numberOfRequests>=maxRequests){
					isFinished = true;
					break;
				}
			}
			
			if(isFinished)
				break;
			
			indexDate = dateUtil.addDays(indexDate, -1);
				
		}
		
		response.setItems(items);
		response.setRequests(numberOfRequests);
		
		return response;
	}

	@Override
	public Response retrieveLocationFeed(LocationFeed feed, Integer maxRequests) throws Exception {
		return new Response();
	}
	
	@Override
	public Response retrieveGroupFeed(GroupFeed feed, Integer maxRequests) {
		return new Response();
	}
	
	public class DateUtil {
	    public Date addDays(Date date, int days) {
	        Calendar cal = Calendar.getInstance();
	        cal.setTime(date);
	        cal.add(Calendar.DATE, days); //minus number decrements the days
	        return cal.getTime();
	    }
	}
	@Override
	public MediaItem getMediaItem(String id) {
		return null;
	}


	@Override
	public StreamUser getStreamUser(String uid) {
		return null;
	}


	@Override
	public Item getItem(String id) {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public List<Item> getItemComments(Item item, long since) {
		// TODO Auto-generated method stub
		return null;
	}

}
