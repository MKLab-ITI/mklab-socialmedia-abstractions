package gr.iti.mklab.framework.retrievers.impl;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.restfb.Connection;
import com.restfb.DefaultFacebookClient;
import com.restfb.FacebookClient;
import com.restfb.Parameter;
import com.restfb.Version;
import com.restfb.exception.FacebookNetworkException;
import com.restfb.exception.FacebookResponseStatusException;
import com.restfb.types.CategorizedFacebookType;
import com.restfb.types.Comment;
import com.restfb.types.Page;
import com.restfb.types.Photo;
import com.restfb.types.Post;
import com.restfb.types.Post.Comments;
import com.restfb.types.User;

import gr.iti.mklab.framework.Credentials;
import gr.iti.mklab.framework.abstractions.socialmedia.items.FacebookItem;
import gr.iti.mklab.framework.abstractions.socialmedia.users.FacebookStreamUser;
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
 * Class responsible for retrieving facebook content based on keywords or facebook users/facebook pages
 * The retrieval process takes place through facebook graph API.
 * 
 * @author manosetro
 * @email  manosetro@iti.gr
 * 
 */
public class FacebookRetriever extends SocialMediaRetriever {
			
	private FacebookClient facebookClient;
	
	private Logger logger = LogManager.getLogger(FacebookRetriever.class);
	
	public FacebookRetriever(Credentials credentials) {
		super(credentials);
		facebookClient = new DefaultFacebookClient(credentials.getAccessToken(), Version.VERSION_2_0);
	}

	@Override
	public Response retrieveAccountFeed(AccountFeed feed, Integer maxRequests) {
		
		List<Item> items = new ArrayList<Item>();

		Integer numberOfRequests = 0;
		
		Date since = new Date(feed.getSinceDate());
		String label = feed.getLabel();
		
		boolean isFinished = false;
		
		String userName = feed.getUsername();
		if(userName == null) {
			logger.error("#Facebook : No source feed");
			Response response = getResponse(items, numberOfRequests);
			return response;
		}
		
		String userFeed = userName + "/feed";
		try {
			logger.info("Retrieve: " + userFeed + " since " + since);
			
			User user = facebookClient.fetchObject(userName, User.class);
			FacebookStreamUser facebookUser = new FacebookStreamUser(user);
			
			Connection<Post> connection = facebookClient.fetchConnection(userFeed, Post.class, Parameter.with("since", since));
			for(List<Post> connectionPage : connection) {
				
				numberOfRequests++;			
				for(Post post : connectionPage) {						
					Date publicationDate = post.getCreatedTime();
					if(post != null && post.getId() != null) {
						
						Item item = new FacebookItem(post, facebookUser);
						if(label != null) {
							item.addLabel(label);
						}
						items.add(item);
					}
					
					Comments postComments = post.getComments();
					if(postComments != null) {
						List<Comment> comments = postComments.getData();
						for(Comment fbComment : comments) {
							Item commentItem = new FacebookItem(fbComment, post, null);
							items.add(commentItem);
						}
					}
					
					if(numberOfRequests > maxRequests) {
						isFinished = true;
						break;
					}
					
					if(publicationDate.before(since)) {
						isFinished = true;
						break;
					}
				
				}
				
				if(isFinished)
					break;
				
			}
		}
		catch(Exception e) {
			e.printStackTrace();
			Response response = getResponse(items, numberOfRequests);
			return response;
		}

		logger.info("Facebook: " + items.size() + " posts from " + userFeed + " [ " + since + " - " + new Date(System.currentTimeMillis()) + " ]");
		
		Response response = getResponse(items, numberOfRequests);
		return response;
	}
	
	@Override
	public Response retrieveKeywordsFeed(KeywordsFeed feed, Integer maxRequests) {
		
		List<Item> items = new ArrayList<Item>();
		Integer numberOfRequests = 0;
		
		Date since = new Date(feed.getSinceDate());
		String label = feed.getLabel();
		
		boolean isFinished = false;
		
		List<String> keywords = feed.getKeywords();
		
		if(keywords == null || keywords.isEmpty()) {
			logger.error("#Facebook : No keywords feed");
			Response response = getResponse(items, numberOfRequests);
			return response;
		}

		StringBuffer query = new StringBuffer();
		for(String keyword : keywords) {
			String [] words = keyword.split(" ");
			for(String word : words) {
				if(word.length() > 1) {
					query.append(word.toLowerCase() + " ");
				}
			}
		}
		
		if(query.length() <= 1) {
			Response response = getResponse(items, numberOfRequests);
			return response;
		}
		
		try {
			logger.info("Query: " + query);
			Connection<Page> connection = facebookClient.fetchConnection("search", Page.class, 
					Parameter.with("q", query), Parameter.with("type", "page"));
			
			try {
				for(Page page : connection.getData()) {	
					logger.info("Page: " + page.getId() + " -> " + page.getName());
					
					numberOfRequests++;
					Connection<Post> pagesConnection = facebookClient.fetchConnection(page.getId()+"/feed", Post.class,
							Parameter.with("since", since));
					
					for(List<Post> connectionPage : pagesConnection) {
						for(Post post : connectionPage) {	

							Date publicationDate = post.getCreatedTime();
							try {
								if(publicationDate.after(since) && post != null && post.getId() != null) {
								
									FacebookItem fbItem;
								
									//Get the user of the post
									CategorizedFacebookType cUser = post.getFrom();
									if(cUser != null) {
										User user = facebookClient.fetchObject(cUser.getId(), User.class);
										StreamUser facebookUser = new FacebookStreamUser(user);
									
										fbItem = new FacebookItem(post, facebookUser);
										if(label != null) {
											fbItem.addLabel(label);
										}
									}
									else {
										fbItem = new FacebookItem(post);
										if(label != null) {
											fbItem.addLabel(label);
										}
									}
								
									items.add(fbItem);
								}
							}
							catch(Exception e) {
								logger.error(e.getMessage());
								break;
							}
						
							if(publicationDate.before(since)){
								isFinished = true;
								break;
							}
						}
						
					}
					if(isFinished) {
						break;
					}
				}

			}
			catch(FacebookNetworkException e){
				logger.error(e.getMessage());
				Response response = getResponse(items, numberOfRequests);
				return response;
			}
		}
		catch(FacebookResponseStatusException e) {
			logger.error(e.getMessage());
			Response response = getResponse(items, numberOfRequests);
			return response;
		}
		catch(Exception e) {
			logger.error(e.getMessage());
			Response response = getResponse(items, numberOfRequests);
			return response;
		}
		
		logger.info("Facebook: " + items.size() + " posts for " + query + " [ " + since + " - " + new Date(System.currentTimeMillis()) + " ]");
		
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
		if(facebookClient != null)
			facebookClient = null;
	}

	@Override
	public MediaItem getMediaItem(String mediaId) {
		
		Photo photo = facebookClient.fetchObject(mediaId, Photo.class);
		
		if(photo == null) {
			return null;
		}
		
		MediaItem mediaItem = null;
		try {
			String src = photo.getSource();
			mediaItem = new MediaItem(new URL(src));
			mediaItem.setId("Facebook#" + photo.getId());
			
			mediaItem.setPageUrl(photo.getLink());
			mediaItem.setThumbnail(photo.getPicture());
			
			mediaItem.setSource("Facebook");
			mediaItem.setType("image");
			
			mediaItem.setTitle(photo.getName());
			
			Date date = photo.getCreatedTime();
			mediaItem.setPublicationTime(date.getTime());
			
			mediaItem.setSize(photo.getWidth(), photo.getHeight());
			mediaItem.setLikes((long) photo.getLikes().size());
			
			CategorizedFacebookType from = photo.getFrom();
			if(from != null) {
				StreamUser streamUser = new FacebookStreamUser(from);
				mediaItem.setUser(streamUser);
				mediaItem.setUserId(streamUser.getUserid());
			}
			
			
		} catch (MalformedURLException e) {
			logger.error(e);
		}
		
		return mediaItem;
	}

	@Override
	public StreamUser getStreamUser(String uid) {
		try {
			Page page = facebookClient.fetchObject(uid, Page.class);
			StreamUser facebookUser = new FacebookStreamUser(page);
			
			return facebookUser;
		}
		catch(Exception e) {
			logger.error(e);
			return null;
		}
	}
	
}
