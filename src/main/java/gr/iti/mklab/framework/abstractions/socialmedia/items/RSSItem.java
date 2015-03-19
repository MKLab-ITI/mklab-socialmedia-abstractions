package gr.iti.mklab.framework.abstractions.socialmedia.items;

import java.util.ArrayList;
import java.util.List;

import org.jsoup.Jsoup;
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

	// URIs
	private static String mrss = "http://search.yahoo.com/mrss/";
	private static String slash = "http://purl.org/rss/1.0/modules/slash/";
	
	public RSSItem(SyndEntry syndEntry) {
		
		if(syndEntry == null || syndEntry.getLink() == null)
			return;
		
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
		
		mediaItems = getMediaItems(syndEntry);
		
		Slash slashModule = (Slash) syndEntry.getModule(slash);
		if(slashModule != null) {
			comments = (long) slashModule.getComments();
		}

	}
	
	private List<MediaItem> getMediaItems(SyndEntry syndEntry) {
		List<MediaItem> mediaItems = new ArrayList<MediaItem>();
		
		@SuppressWarnings("unchecked")
		List<SyndEnclosure> enclosures = syndEntry.getEnclosures();
		for(SyndEnclosure encl : enclosures) {
			MediaItem mi = new MediaItem();
			
			mi.setUrl(encl.getUrl());
			
			String type = encl.getType();
			if(type.contains("image")) {
				mi.setType("image");
			}
			else if(type.contains("video")) {
				mi.setType("video");
			}
			
			mediaItems.add(mi);
		}
		
		MediaEntryModule module = (MediaEntryModule) syndEntry.getModule(mrss);
		if(module != null) {
			MediaContent[] mediaContents = module.getMediaContents();
			for(MediaContent mediaContent : mediaContents) {
				MediaItem mi = new MediaItem();
				mi.setUrl(mediaContent.getReference().toString());
				mi.setType(mediaContent.getMedium());
				
				Metadata metadata = mediaContent.getMetadata();
				mi.setTitle(metadata.getTitle()); 
				mi.setDescription(metadata.getDescription());
				
				if(mediaContent.getWidth() != null && mediaContent.getHeight() != null) {
					mi.setSize(mediaContent.getWidth(), mediaContent.getHeight());
				}
				
				mediaItems.add(mi);
			}
		}
		return mediaItems;
	}
	
	private String extractText(String content) {
		org.jsoup.nodes.Document doc = Jsoup.parse(content);
		String text = doc.body().text();
		return text;
	}
	
}
