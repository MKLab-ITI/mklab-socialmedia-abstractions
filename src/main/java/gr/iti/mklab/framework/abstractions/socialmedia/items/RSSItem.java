package gr.iti.mklab.framework.abstractions.socialmedia.items;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.sun.syndication.feed.module.content.ContentModule;
import com.sun.syndication.feed.module.slash.Slash;
import com.sun.syndication.feed.module.mediarss.MediaEntryModule;
import com.sun.syndication.feed.module.mediarss.types.MediaContent;
import com.sun.syndication.feed.module.mediarss.types.Metadata;
import com.sun.syndication.feed.synd.SyndCategory;
import com.sun.syndication.feed.synd.SyndEnclosure;
import com.sun.syndication.feed.synd.SyndEntry;

import gr.iti.mklab.framework.common.domain.Item;
import gr.iti.mklab.framework.common.domain.MediaItem;

/**
 * Class that holds the information of an RSS feed
 * 
 * @author Manos Schinas
 * 
 * @author manosetro@iti.gr
 */
public class RSSItem extends Item {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1413164596016357110L;

	// URIs of RSS modules
	private static String mrss = "http://search.yahoo.com/mrss/";
	private static String slash = "http://purl.org/rss/1.0/modules/slash/";
	private static String content = "http://purl.org/rss/1.0/modules/content/";
			
	public RSSItem(SyndEntry syndEntry) {
		
		if(syndEntry == null || syndEntry.getLink() == null) {
			return;
		}
		
		//Id
		id = syndEntry.getLink();
		
		//Document's title
		title = syndEntry.getTitle();
		
		//description = rssEntry.getDescription().getValue();
		//Document's content - Extract text content from html structure
		if(syndEntry.getDescription() != null) {
			description = extractText(syndEntry.getDescription().getValue());
		}
		
		//Document's time of publication
		publicationTime = syndEntry.getPublishedDate().getTime();
		
		//The URL where the document can be found
		url = syndEntry.getLink();
		
		uid = syndEntry.getAuthor();
		
		@SuppressWarnings("unchecked")
		List<SyndCategory> syndCategories = syndEntry.getCategories();
		if(syndCategories != null) {
			List<String> categories = new ArrayList<String>();
			for(SyndCategory category : syndCategories) {
				categories.add(category.getName());
			}
			tags = categories.toArray(new String[categories.size()]);
		}
		
		Map<String, MediaItem> mediaItemsMap = getMediaItems(syndEntry);
		mediaItems.addAll(mediaItemsMap.values());
		mediaIds.addAll(mediaItemsMap.keySet());
		
		Slash slashModule = (Slash) syndEntry.getModule(slash);
		if(slashModule != null) {
			comments = (long) slashModule.getComments();
		}

		String textContent = getContent(syndEntry);
		if(textContent != null) {
			text = extractText(textContent);
			
			Map<String, MediaItem> mediaFromContent = extractMedia(textContent, url);
			mediaItems.addAll(mediaFromContent.values());
			mediaIds.addAll(mediaFromContent.keySet());
		}
		
	}
	
	private String getContent(SyndEntry syndEntry) {
		StringBuffer contentBuffer = new StringBuffer();
		
		ContentModule contentModule = (ContentModule) syndEntry.getModule(content);
		if(contentModule == null)
			return null;
		
		@SuppressWarnings("unchecked")
		List<String> contents = contentModule.getContents();
		for(Object contentPart : contents) {
			contentBuffer.append(contentPart);
		}
		
		return contentBuffer.toString();
	}
	
	private Map<String, MediaItem> getMediaItems(SyndEntry syndEntry) {
		
		String referenceId = syndEntry.getLink();
		
		Map<String, MediaItem> mediaItems = new HashMap<String, MediaItem>();
		
		@SuppressWarnings("unchecked")
		List<SyndEnclosure> enclosures = syndEntry.getEnclosures();
		for(SyndEnclosure encl : enclosures) {
			MediaItem mi = new MediaItem();
			
			mi.setId(encl.getUrl());
			mi.setUrl(encl.getUrl());
			mi.setReference(referenceId);
			
			String type = encl.getType();
			if(type.contains("image")) {
				mi.setType("image");
			}
			else if(type.contains("video")) {
				mi.setType("video");
			}
			
			mediaItems.put(mi.getId(), mi);
		}
		
		MediaEntryModule module = (MediaEntryModule) syndEntry.getModule(mrss);
		if(module != null) {
			MediaContent[] mediaContents = module.getMediaContents();
			for(MediaContent mediaContent : mediaContents) {
				
				String mediaUrl = mediaContent.getReference().toString();
				
				MediaItem mi = new MediaItem();
				mi.setId(mediaUrl);
				mi.setUrl(mediaUrl);
				mi.setReference(referenceId);
				
				mi.setType(mediaContent.getMedium());
				
				Metadata metadata = mediaContent.getMetadata();
				mi.setTitle(metadata.getTitle()); 
				mi.setDescription(metadata.getDescription());
				mi.setTags(metadata.getKeywords());
				
				if(mediaContent.getWidth() != null && mediaContent.getHeight() != null) {
					if(mediaContent.getWidth()>1 && mediaContent.getHeight()>1) {
						mi.setSize(mediaContent.getWidth(), mediaContent.getHeight());
					}
				}
				
				mediaItems.put(mi.getId(), mi);
			}
		}
		return mediaItems;
	}
	
	private String extractText(String content) {
		org.jsoup.nodes.Document doc = Jsoup.parse(content);
		String text = doc.body().text();
		return text;
	}
	
	private Map<String, MediaItem> extractMedia(String content, String base) {
		Map<String, MediaItem> mediaItems = new HashMap<String, MediaItem>();
		
		org.jsoup.nodes.Document doc = Jsoup.parse(content);
		Elements imgElements = doc.getElementsByTag("img");
		for(int index = 0; index<imgElements.size(); index++) {
			Element img = imgElements.get(index);
			
			MediaItem mi = new MediaItem();
			
			mi.setId(img.attr("src"));
			mi.setUrl(img.attr("src"));
			mi.setTitle(img.attr("alt"));
			mi.setReference(base);
			mi.setType("image");
			
			String w = img.attr("width");
			String h = img.attr("height");
			if(w != null && h != null && !w.equals("") && !h.equals("")) {
				try {
					int width = Integer.parseInt(w); 
					int height = Integer.parseInt(h);
					if(width>1 && height>1) {
						mi.setSize(width, height);
					}
				}
				catch(Exception e) {}
			}
					
			mediaItems.put(mi.getId(), mi);
		}
		
		return mediaItems;
	}
	
}
