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

import org.apache.log4j.Logger;
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
import gr.iti.mklab.framework.retrievers.SocialMediaRetriever;

/**
 * Class responsible for retrieving Tumblr content based on keywords or tumblr users
 * The retrieval process takes place through Tumblr API (Jumblr).
 * 
 * @author manosetro
 * @email  manosetro@iti.gr
 */
public class TumblrRetriever extends SocialMediaRetriever {
	
	private Logger logger = Logger.getLogger(TumblrRetriever.class);
	
	private JumblrClient client;
	
	public TumblrRetriever(Credentials credentials) {
		
		super(credentials);
		
		client = new JumblrClient(credentials.getKey(), credentials.getSecret());
	}

	
	@Override
	public List<Item> retrieveAccountFeed(AccountFeed feed, Integer maxResults, Integer maxRequests){
		List<Item> items = new ArrayList<Item>();
		
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
		Date lastItemDate = feed.getSinceDate();
		
		int numberOfRequests = 0;
		
		boolean isFinished = false;

		String uName = feed.getUsername();
		
		if(uName == null){
			logger.info("#Tumblr : No source feed");
			return null;
		}
		
		Blog blog = client.blogInfo(uName);
		TumblrStreamUser tumblrStreamUser = new TumblrStreamUser(blog);
		List<Post> posts;
		Map<String,String> options = new HashMap<String,String>();
		
		Integer offset = 0;
		Integer limit = 20;
		options.put("limit", limit.toString());
	
		while(true){
			
			options.put("offset", offset.toString());
			
			posts = blog.posts(options);
			if(posts == null || posts.isEmpty())
				break;
			
			numberOfRequests ++;
			
			for(Post post : posts){
				
				if(post.getType().equals("photo") || post.getType().equals("video") || post.getType().equals("link")){
					
					String retrievedDate = post.getDateGMT().replace(" GMT", "");
					retrievedDate+=".0";
					
					Date publicationDate = null;
					try {
						publicationDate = (Date) formatter.parse(retrievedDate);
						
					} catch (ParseException e) {
						return items;
					}
					
					if(publicationDate.after(lastItemDate) && post != null && post.getId() != null){
						
						TumblrItem tumblrItem = null;
						try {
							tumblrItem = new TumblrItem(post,tumblrStreamUser);
						} catch (MalformedURLException e) {
							
							return items;
						}
						
						items.add(tumblrItem);
						
					}
				
				}
				if(items.size()>maxResults || numberOfRequests>maxRequests){
					isFinished = true;
					break;
				}
			}
			if(isFinished)
				break;
			
			offset+=limit;
		}

		//logger.info("#Tumblr : Done retrieving for this session");
//		logger.info("#Tumblr : Handler fetched " +totalRetrievedItems + " posts from " + uName + 
//				" [ " + lastItemDate + " - " + new Date(System.currentTimeMillis()) + " ]");
		
		return items;
	}
	
	@Override
	public List<Item> retrieveKeywordsFeed(KeywordsFeed feed, Integer maxResults, Integer maxRequests) {
		
		List<Item> items = new ArrayList<Item>();
		
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
		Date currentDate = new Date(System.currentTimeMillis());
		Date indexDate = currentDate;
		Date lastItemDate = feed.getSinceDate();
		DateUtil dateUtil = new DateUtil();
		
		int numberOfRequests=0;
		
		boolean isFinished = false;
		
		List<String> keywords = feed.getKeywords();
		
		if(keywords == null || keywords.isEmpty()) {
			logger.info("#Tumblr : No keywords feed");
			return items;
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
			return items;
		
		while(indexDate.after(lastItemDate) || indexDate.equals(lastItemDate)){
			
			Map<String,String> options = new HashMap<String,String>();
			Long checkTimestamp = indexDate.getTime();
			Integer check = checkTimestamp.intValue();
			options.put("featured_timestamp", check.toString());
			List<Post> posts;
			try{
				posts = client.tagged(tags);
			}catch(JumblrException e){
				return items;
			}catch(OAuthConnectionException e1){
				return items;
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
						return items;
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
							return items;
						}
						
						if(tumblrItem != null){
							items.add(tumblrItem);
						}
						
					}
				
				}
				
				if(items.size()>maxResults || numberOfRequests>=maxRequests){
					isFinished = true;
					break;
				}
			}
			
			if(isFinished)
				break;
			
			indexDate = dateUtil.addDays(indexDate, -1);
				
		}
		
		return items;
		
	}

	@Override
	public List<Item> retrieveLocationFeed(LocationFeed feed, Integer maxRequests, Integer maxResults) throws Exception {
		return new ArrayList<Item>();
	}
	
	@Override
	public List<Item> retrieveGroupFeed(GroupFeed feed, Integer maxResults, Integer maxRequests) {
		return new ArrayList<Item>();
	}
	
	@Override
	public void stop() {
		if(client != null){
			client = null;
		}
	}
	public class DateUtil
	{
	    public Date addDays(Date date, int days)
	    {
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

}
