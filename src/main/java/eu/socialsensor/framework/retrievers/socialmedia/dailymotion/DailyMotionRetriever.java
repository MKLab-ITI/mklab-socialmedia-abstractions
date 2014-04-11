package eu.socialsensor.framework.retrievers.socialmedia.dailymotion;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.JsonObjectParser;
import com.google.api.client.json.jackson.JacksonFactory;
import com.google.api.client.util.Key;

import eu.socialsensor.framework.abstractions.socialmedia.dailymotion.DailyMotionMediaItem;
import eu.socialsensor.framework.abstractions.socialmedia.dailymotion.DailyMotionMediaItem.DailyMotionVideo;
import eu.socialsensor.framework.common.domain.Feed;
import eu.socialsensor.framework.common.domain.MediaItem;
import eu.socialsensor.framework.common.domain.feeds.KeywordsFeed;
import eu.socialsensor.framework.common.domain.feeds.LocationFeed;
import eu.socialsensor.framework.common.domain.feeds.SourceFeed;
import eu.socialsensor.framework.retrievers.socialmedia.SocialMediaRetriever;

/**
 * The retriever that implements the Daily Motion wrapper
 * @author manosetro
 * @email  manosetro@iti.gr
 */
public class DailyMotionRetriever implements SocialMediaRetriever {

	static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
	static final JsonFactory JSON_FACTORY = new JacksonFactory();

	private HttpRequestFactory requestFactory;
	private String requestPrefix = "https://api.dailymotion.com/video/";
	
	
	public DailyMotionRetriever() {
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
	public Integer retrieve(Feed feed) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Integer retrieveKeywordsFeeds(KeywordsFeed feed) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Integer retrieveUserFeeds(SourceFeed feed) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Integer retrieveLocationFeeds(LocationFeed feed) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}
}