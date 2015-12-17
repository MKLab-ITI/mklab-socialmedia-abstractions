package gr.iti.mklab.framework.retrievers.impl;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import com.restfb.util.StringUtils;
import com.sun.syndication.feed.synd.SyndEntry;
import com.sun.syndication.feed.synd.SyndFeed;
import com.sun.syndication.fetcher.FeedFetcher;
import com.sun.syndication.fetcher.impl.FeedFetcherCache;
import com.sun.syndication.fetcher.impl.HashMapFeedInfoCache;
import com.sun.syndication.fetcher.impl.HttpURLFeedFetcher;

import gr.iti.mklab.framework.abstractions.socialmedia.items.RSSItem;
import gr.iti.mklab.framework.common.domain.Item;
import gr.iti.mklab.framework.common.domain.feeds.Feed;
import gr.iti.mklab.framework.common.domain.feeds.RssFeed;
import gr.iti.mklab.framework.retrievers.Response;
import gr.iti.mklab.framework.retrievers.Retriever;

/**
 * Class for retrieving RSS feeds using Rome API.
 *  
 * @author Manos Schinas
 * 
 * @email manosetro@iti.gr
 * 
 */
public class RssRetriever implements Retriever {
	
	public final Logger logger = LogManager.getLogger(RssRetriever.class);
	
	private FeedFetcherCache cache = HashMapFeedInfoCache.getInstance();
	private FeedFetcher feedFetcher = new HttpURLFeedFetcher(cache);
	
	@Override
	public Response retrieve(Feed feed) throws Exception {
		return retrieve(feed, 1);
	}
		
	@Override
	public Response retrieve(Feed feed, Integer maxRequests) throws Exception {

		Response response = new Response();
		List<Item> items = new ArrayList<Item>();
		
		if(RssFeed.class.isInstance(feed.getClass())) {
			logger.error("Feed " + feed.getClass() + "is not instance of Rss Feed");
			throw new Exception("Feed " + feed.getClass() + "is not instance of Rss Feed");
		}
		
		RssFeed rrsFeed = (RssFeed) feed;
		logger.info("["+new Date()+"] Retrieving RSS Feed: " + rrsFeed.getURL());
		
		if(rrsFeed.getURL().equals("")) {
			logger.error("URL is null");
			response.setItems(items);
			return response;
		}
		
		Date since = new Date(rrsFeed.getSinceDate());
		
		try {
			URL url = new URL(rrsFeed.getURL());
			
			SyndFeed syndFeed;
			synchronized(feedFetcher) {
				syndFeed = feedFetcher.retrieveFeed(url);
			}
			
			
			String sourceLink = syndFeed.getLink();
			URL sourceURL = new URL(sourceLink);

			@SuppressWarnings("unchecked")
			List<SyndEntry> entries = syndFeed.getEntries();
			
			for (SyndEntry entry : entries) {		
				if(entry.getLink() != null) {
					
					Date publicationDate = entry.getPublishedDate();
					if(publicationDate.before(since)) {
						logger.info(publicationDate + " before " + since);
						break;
					}
					
					Item item = new RSSItem(entry);
					item.setSource(sourceURL.getHost());
					
					String label = feed.getLabel();
					if(label != null) {
						item.addLabel(label);
					}	
					
					items.add(item);			
				}
			}
			
		} catch (MalformedURLException e) {
			logger.error(e);
		} catch (IOException e) {
			logger.error(e);
		} catch (Exception e) {
			logger.error(e);
		}
	
		response.setItems(items);
		return response;
	}

	
	@Override
	public void stop() {
	
	}
	
	public static void main(String...args) throws Exception {
		
		String id = "businessgreen";
		String url = "http://www.treehugger.com/feeds/latest/";		
		String source = "RSS";
		
		long since = System.currentTimeMillis() - 90*24*3600*1000L;
		
		RssFeed feed = new RssFeed(id, url, since, source);
			
		RssRetriever retriever = new RssRetriever();
		Response response = retriever.retrieve(feed);
		
		System.out.println(response.getNumberOfItems());
		for(Item item : response.getItems()) {
			System.out.println("ID: " + item.getId());
			System.out.println("Title: " + item.getTitle());
			System.out.println(new Date(item.getPublicationTime()));
			System.out.println("Description: " + item.getDescription());
			System.out.println("User: " + item.getUserId());
			System.out.println("Url: " + item.getPageUrl());
			System.out.println("MediaIds: " + item.getMediaIds());
			System.out.println("Tags: " + StringUtils.join(item.getTags()));
			System.out.println(item.getMediaItems());
			System.out.println("Comments: " + item.getComments());
			System.out.println("====================================");
		}
	}
	
}
