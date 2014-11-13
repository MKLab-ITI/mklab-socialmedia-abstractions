package gr.iti.mklab.framework.retrievers.impl;


import java.util.ArrayList;
import java.util.List;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.JsonObjectParser;
import com.google.api.client.json.jackson2.JacksonFactory;

import gr.iti.mklab.framework.abstractions.socialmedia.mediaitems.TwitPicMediaItem.TwitPicImage;
import gr.iti.mklab.framework.abstractions.socialmedia.mediaitems.TwitPicMediaItem;
import gr.iti.mklab.framework.common.domain.Feed;
import gr.iti.mklab.framework.common.domain.Item;
import gr.iti.mklab.framework.common.domain.MediaItem;
import gr.iti.mklab.framework.common.domain.StreamUser;
import gr.iti.mklab.framework.common.domain.feeds.KeywordsFeed;
import gr.iti.mklab.framework.common.domain.feeds.ListFeed;
import gr.iti.mklab.framework.common.domain.feeds.LocationFeed;
import gr.iti.mklab.framework.common.domain.feeds.SourceFeed;
import gr.iti.mklab.framework.retrievers.SocialMediaRetriever;

/**
 * The retriever that implements the Twitpic simplified retriever
 * @author manosetro
 * @email  manosetro@iti.gr
 */
public class TwitpicRetriever extends SocialMediaRetriever {

	private static String requestPrefix = "http://api.twitpic.com/2/media/show.json?id=";
	
	static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
	static final JsonFactory JSON_FACTORY = new JacksonFactory();
	
	private HttpRequestFactory requestFactory;

	public TwitpicRetriever() {
		
		super(null, null, null);
		
		requestFactory = HTTP_TRANSPORT.createRequestFactory(
				new HttpRequestInitializer() {
					@Override
					public void initialize(HttpRequest request) {
						request.setParser(new JsonObjectParser(JSON_FACTORY));
					}
				});
	}
	
	public MediaItem getMediaItem(String shortId) {
		
		GenericUrl requestUrl = new GenericUrl(requestPrefix + shortId);
		
		HttpRequest request;
		try {
			request = requestFactory.buildGetRequest(requestUrl);
			HttpResponse response = request.execute();
			TwitPicImage image = response.parseAs(TwitPicImage.class);
			if(image != null) {
				MediaItem mediaItem = new TwitPicMediaItem(image);
				return mediaItem;
			}
		} catch (Exception e) {
		}
		
		return null;
	}

	@Override
	public List<Item> retrieve(Feed feed) {
		return new ArrayList<Item>();
	}

	@Override
	public void stop() {
		
	}

	@Override
	public List<Item> retrieveKeywordsFeeds(KeywordsFeed feed) throws Exception {
		return new ArrayList<Item>();
	}

	@Override
	public List<Item> retrieveUserFeeds(SourceFeed feed) throws Exception {
		return new ArrayList<Item>();
	}

	@Override
	public List<Item> retrieveLocationFeeds(LocationFeed feed) throws Exception {
		return new ArrayList<Item>();
	}

	@Override
	public StreamUser getStreamUser(String uid) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Item> retrieveListsFeeds(ListFeed feed) {
		return new ArrayList<Item>();
	}

	@Override
	public List<Item> retrieve(Feed feed, Integer maxRequests,
			Integer maxResults) {
		return null;
	}

	@Override
	public List<Item> retrieveKeywordsFeeds(KeywordsFeed feed,
			Integer maxRequests, Integer maxResults) throws Exception {
		return null;
	}

	@Override
	public List<Item> retrieveUserFeeds(SourceFeed feed, Integer maxRequests,
			Integer maxResults) throws Exception {
		return null;
	}

	@Override
	public List<Item> retrieveLocationFeeds(LocationFeed feed,
			Integer maxRequests, Integer maxResults) throws Exception {
		return null;
	}

	@Override
	public List<Item> retrieveListsFeeds(ListFeed feed, Integer maxRequests,
			Integer maxResults) {
		return null;
	}

}
