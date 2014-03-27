package eu.socialsensor.framework.streams;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

import eu.socialsensor.framework.common.domain.Feed;
import eu.socialsensor.framework.common.domain.Item;
import eu.socialsensor.framework.common.domain.StreamUser.Category;
import eu.socialsensor.framework.monitors.FeedsMonitor;
import eu.socialsensor.framework.retrievers.Retriever;
import eu.socialsensor.framework.subscribers.socialmedia.Subscriber;



/**
 * Class handles the stream of information regarding a social network.
 * It is responsible for its configuration, its wrapper's initialization
 * and its retrieval process.
 * @author manosetro
 * @email  manosetro@iti.gr
 * @author ailiakop
 * @email  ailiakop@iti.gr
 *
 */
public abstract class Stream implements Runnable {

	protected static final String KEY = "Key";
	protected static final String SECRET = "Secret";
	protected static final String ACCESS_TOKEN = "AccessToken";
	protected static final String ACCESS_TOKEN_SECRET = "AccessTokenSecret";
	protected static final String CLIENT_ID = "ClientId";
	protected static final String MAX_RESULTS = "maxResults";
	protected static final String MAX_REQUESTS = "maxRequests";
	
	protected FeedsMonitor monitor;
	protected BlockingQueue<Feed> feedsQueue;
	protected Retriever retriever = null;
	protected Subscriber subscriber = null;
	protected StreamHandler handler;
	
	private Logger  logger = Logger.getLogger(Stream.class);
	
	protected boolean isSubscriber = false;
	
	private Map<String, Set<String>> usersToLists;
	private Map<String,Category> usersToCategory;
	
	/**
	 * Open a stream for updates delivery
	 * @param config
	 *      Stream configuration parameters
	 * @throws StreamException
	 *      In any case of error during stream open
	 */
	public abstract void open(StreamConfiguration config) throws StreamException;
	
	/**
	 * Close a stream 
	 * @throws StreamException
	 *      In any case of error during stream close
	 */
	public void close() throws StreamException {
		if(monitor != null)
			monitor.stopMonitor();
		if(retriever !=null)
			retriever.stop();
		if(subscriber != null)
			subscriber.stop();
		
		
		logger.info("Close Stream  : "+this.getClass().getName());
	}
	
	
	/**
	 * Set the handler that is responsible for the handling 
	 * of the retrieved items
	 * @param handler
	 */
	public void setHandler(StreamHandler handler){
		this.handler = handler;
	}
	
	/**
	 * Sets the feeds monitor for the stream
	 * @return
	 */
	public boolean setMonitor(){
		if(retriever == null)
			return false;
		
		monitor = new FeedsMonitor(retriever);
		return true;
	}
	
	public void setUserLists(Map<String, Set<String>> usersToLists) {
		this.usersToLists = usersToLists;
		
		Set<String> allLists = new HashSet<String>();
		for(Set<String> lists : usersToLists.values()) {
			allLists.addAll(lists);
		}
		logger.info("=============================================");
		logger.info(usersToLists.size() + " user in " + allLists.size() + " Lists!!!");
	}
	
	public void setUserCategories(Map<String, Category> usersToCategory) {
		this.usersToCategory = usersToCategory;
	}
	
	public void setAsSubscriber(){
		this.isSubscriber = true;
	}
	
	public synchronized void stream(List<Feed> feeds) throws StreamException {
		
		if(subscriber != null){
			subscriber.subscribe(feeds);
		}
		
	}
	
	/**
	 * Searches with the wrapper of the stream for a particular
	 * set of feeds (feeds can be keywordsFeeds, userFeeds or locationFeeds)
	 * @param feeds
	 * @return the total number of retrieved items for the stream
	 * @throws StreamException
	 */
	public synchronized Integer poll(List<Feed> feeds) throws StreamException {
		Integer totalRetrievedItems = 0;
		
		if(retriever != null) {
		
			if(feeds == null)
				return totalRetrievedItems;
				
			for(Feed feed : feeds){
				
				totalRetrievedItems += retriever.retrieve(feed);
				
			}
			
			
			logger.info("Retrieved items for "+this.getClass().getName()+ " are : "+totalRetrievedItems);
		}
		return totalRetrievedItems;
	}
	
	
	
	/**
	 * Store a set of items in the selected databases
	 * @param items
	 */
	public synchronized void store(List<Item>items) {
		for(Item item : items) {
			store(item);
		}
	}
	
	/**
	 * Store an item in the selected databases
	 * @param item
	 */
	public synchronized void store(Item item) {
		if(handler == null) {
			logger.error("NULL Handler!");
			return;
		}
			
		if(usersToLists != null && getUserList(item) != null)
			item.setList(getUserList(item));
		
		if(usersToCategory != null && getUserCategory(item) != null)
			item.setCategory(getUserCategory(item));
		
		handler.update(item);
	}
	
	private String[] getUserList(Item item) {
		
		Set<String> lists = new HashSet<String>();
		if(usersToLists == null){
			logger.error("User list is null");
			return null;
		}
			
		if(item.getUserId() == null){
			logger.error("User in item is null");
			return null;
		}
				
		Set<String> userLists = usersToLists.get(item.getUserId());
		if(userLists != null) {
			lists.addAll(userLists);
		}
		
		for(String mention : item.getMentions()) {
			userLists = usersToLists.get(mention);
			if(userLists != null) {
				lists.addAll(userLists);
			}
		}
		
		String refUserId = item.getReferencedUserId();
		if(refUserId != null) {
			userLists = usersToLists.get(refUserId);
			if(userLists != null) {
				lists.addAll(userLists);
			}
		}
		
		if(lists.size() > 0) {
			//logger.info(item.getId() + " is associated with " + lists);
			return lists.toArray(new String[lists.size()]);
		}
		else {
			//logger.info("Any list found for " + item.getId());
			return null;
		}
		
	}
	
	private Category getUserCategory(Item item){
		
		if(usersToCategory == null){
			logger.error("User categories is null");
			return null;
		}
			
		if(item.getUserId() == null){
			logger.error("User in item is null");
			return null;
		}
			
		
		return usersToCategory.get(item.getUserId());
		
	}
	
	/**
	 * Deletes an item from the selected databases
	 * @param item
	 */
	public void delete(Item item){
		handler.delete(item);
	}
	
	/**
	 * Adds a feed to the stream for future searching
	 * @param feed
	 * @return
	 */
	public boolean addFeed(Feed feed) {
		if(feedsQueue == null)
			return false;
		
		return feedsQueue.offer(feed);
	}
	
	@Override
	public void run() {
		while(true) {
			try {
				Feed feed = feedsQueue.take();
				monitor.addFeed(feed);
				monitor.startMonitor(feed);
				
			} catch (InterruptedException e) {
				return;
			}
		}
	}
}
