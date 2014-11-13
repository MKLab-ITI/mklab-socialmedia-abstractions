package gr.iti.mklab.framework.retrievers.impl;

import java.util.ArrayList;
import java.util.List;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.JsonObjectParser;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.Key;

import gr.iti.mklab.framework.abstractions.socialmedia.mediaitems.DailyMotionMediaItem;
import gr.iti.mklab.framework.abstractions.socialmedia.mediaitems.DailyMotionMediaItem.DailyMotionVideo;
import gr.iti.mklab.framework.common.domain.Item;
import gr.iti.mklab.framework.common.domain.MediaItem;
import gr.iti.mklab.framework.common.domain.StreamUser;
import gr.iti.mklab.framework.common.domain.feeds.KeywordsFeed;
import gr.iti.mklab.framework.common.domain.feeds.ListFeed;
import gr.iti.mklab.framework.common.domain.feeds.LocationFeed;
import gr.iti.mklab.framework.common.domain.feeds.SourceFeed;
import gr.iti.mklab.framework.retrievers.SocialMediaRetriever;

/**
 * The retriever that implements the Daily Motion wrapper
 * @author manosetro
 * @email  manosetro@iti.gr
 */
public class DailyMotionRetriever extends SocialMediaRetriever {

	static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
	static final JsonFactory JSON_FACTORY = new JacksonFactory();

	private HttpRequestFactory requestFactory;
	private String requestPrefix = "https://api.dailymotion.com/video/";
	
	public DailyMotionRetriever() {
		
		super(null, null, null);
		
		requestFactory = HTTP_TRANSPORT.createRequestFactory(
				new HttpRequestInitializer() {
					@Override
					public void initialize(HttpRequest request) {
						request.setParser(new JsonObjectParser(JSON_FACTORY));
					}
				});
	}
	
	/** 
	 * URL for Dailymotion API. 
	 */
	private static class DailyMotionUrl extends GenericUrl {

		public DailyMotionUrl(String encodedUrl) {
			super(encodedUrl);
		}

		@Key
		public String fields = "id,tags,title,url,embed_url,rating,thumbnail_url," +
				"views_total,created_time,geoloc,ratings_total,comments_total";
	}
	
	/**
	 * Returns the retrieved media item
	 */
	public MediaItem getMediaItem(String id) {
		
		DailyMotionUrl url = new DailyMotionUrl(requestPrefix + id);
		
		HttpRequest request;
		try {
			request = requestFactory.buildGetRequest(url);
			DailyMotionVideo video = request.execute().parseAs(DailyMotionVideo.class);
			
			if(video != null) {
				MediaItem mediaItem = new DailyMotionMediaItem(video);
				return mediaItem;
			}
			
		} catch (Exception e) {
			
		}

		return null;
	}

	@Override
	public void stop() {
		
	}

	@Override
	public List<Item> retrieveKeywordsFeeds(KeywordsFeed feed, Integer maxRequests, Integer maxResults) throws Exception {
		return new ArrayList<Item>();
	}

	@Override
	public List<Item> retrieveUserFeeds(SourceFeed feed, Integer maxRequests, Integer maxResults) throws Exception {
		return new ArrayList<Item>();
	}

	@Override
	public List<Item> retrieveLocationFeeds(LocationFeed feed, Integer maxRequests, Integer maxResults) throws Exception {
		return new ArrayList<Item>();
	}

	@Override
	public StreamUser getStreamUser(String uid) {
		return null;
	}

	@Override
	public List<Item> retrieveListsFeeds(ListFeed feed, Integer maxRequests, Integer maxResults) {
		return new ArrayList<Item>();
	}
}