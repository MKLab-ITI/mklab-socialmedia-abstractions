package gr.iti.mklab.framework.abstractions.socialmedia.items;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.google.api.services.plus.model.Activity;
import com.google.api.services.plus.model.Activity.Actor;
import com.google.api.services.plus.model.Activity.PlusObject;
import com.google.api.services.plus.model.Activity.PlusObject.Attachments;
import com.google.api.services.plus.model.Activity.PlusObject.Attachments.Embed;
import com.google.api.services.plus.model.Activity.PlusObject.Attachments.FullImage;
import com.google.api.services.plus.model.Activity.PlusObject.Attachments.Image;
import com.google.api.services.plus.model.Activity.PlusObject.Attachments.Thumbnails;
import com.google.api.services.plus.model.Comment;
import com.google.api.services.plus.model.Place;
import com.google.api.services.plus.model.Place.Address;
import com.google.api.services.plus.model.Place.Position;

import gr.iti.mklab.framework.abstractions.socialmedia.users.GooglePlusStreamUser;
import gr.iti.mklab.framework.common.domain.Item;
import gr.iti.mklab.framework.common.domain.Location;
import gr.iti.mklab.framework.common.domain.MediaItem;
import gr.iti.mklab.framework.common.domain.Source;
import gr.iti.mklab.framework.common.domain.StreamUser;
import gr.iti.mklab.framework.common.domain.WebPage;

/**
 * Class that holds the information of a google plus activity
 * @author ailiakop
 * @email  ailiakop@iti.gr
 */
public class GooglePlusItem extends Item {
	
	private static final long serialVersionUID = 1077924633642822831L;	
	
	public GooglePlusItem() {
		
	}
	
	public GooglePlusItem(Activity activity) {
		
		if(activity == null || activity.getId() == null) {
			return;
		}
		
		//Id
		id = Source.GooglePlus + "#" + activity.getId();
		
		//SocialNetwork Name
		source = Source.GooglePlus.toString();
		
		//timestamp of the creation of the post
		publicationTime =  activity.getPublished().getValue();
		
		//Title of the post
		title = activity.getTitle();
		
		//User that made the post
        Actor actor = activity.getActor();
        if(actor != null) {
                streamUser = new GooglePlusStreamUser(actor);
                uid = streamUser.getId();
        }
        
		//Location
        if(activity.getLocation() != null) {
        	Place place = activity.getLocation();
        	Address address = place.getAddress();	
        	Position position = place.getPosition();
        	if(address != null && position != null) {
        		Double latitude = position.getLatitude();
            	Double longitude = position.getLongitude();
            	
            	location = new Location(latitude, longitude, address.getFormatted());
        	}
        }
        else if(activity.getGeocode() != null) {
			
			String locationInfo = activity.getGeocode();
			String[] parts = locationInfo.split(" ");
			double latitude = Double.parseDouble(parts[0]);
			double longitude = Double.parseDouble(parts[1]);
			
			location = new Location(latitude, longitude, activity.getPlaceName());
		}
		
		PlusObject object = activity.getObject();
		
		String objectType = object.getObjectType();
		if(objectType.equals("note")) {
			original = true;
		}
		else if(objectType.equals("activity")) {
			original = false;
			reference = Source.GooglePlus + "#" + object.getId();
			referencedUserId = Source.GooglePlus + "#" + object.getActor().getId();
		}
		
		description = object.getContent();
		if(description != null) {
			try {
				// extract tags from text
				List<String> tagsList = new ArrayList<String>();
				Document doc = Jsoup.parse(description);
				Elements elements = doc.getElementsByClass("ot-hashtag");
				for(Element element : elements) {
					String tag = element.text();
					if(tag != null) {
						tagsList.add(tag.replaceAll("#", ""));
					}
				}
				tags = tagsList.toArray(new String[tagsList.size()]);
			}
			catch(Exception e) {
			}
		}
		
		//Popularity
		if(object.getPlusoners() != null) {
			likes = object.getPlusoners().getTotalItems();
		}
			
		if(activity.getObject().getResharers() != null) {
			shares = object.getResharers().getTotalItems();
		}
			
		if(activity.getObject().getReplies() != null) {
			comments = object.getReplies().getTotalItems();
		}
		
		pageUrl = activity.getUrl();
		if(pageUrl == null) {
			pageUrl = object.getUrl();
		}
		
		//Media Items - WebPages in a post
		webPages = new ArrayList<WebPage>();

		List<Attachments> attachmentsList = object.getAttachments();
		if(attachmentsList != null) {
			for(Attachments attachment : attachmentsList) {
				String type = attachment.getObjectType();
				
				if(type == null) {
					continue;
				}
				
				//possible types: video, photo, album, article
				if(type.equals("video")) {
		    		MediaItem mediaItem = this.getMediaItemFromVideoAttachment(attachment);
		    		if(mediaItem != null) {
			    		mediaItems.add(mediaItem);
			    		mediaIds.add(mediaItem.getId());	
		    		}	
		    	}	
		    	else if(type.equals("photo")) {				    		
		    		MediaItem mediaItem = this.getMediaItemFromPhotoAttachment(attachment);
		    		if(mediaItem != null) {
			    		mediaItems.add(mediaItem);
			    		mediaIds.add(mediaItem.getId());	
		    		}	
		    	}
		    	else if(type.equals("album")) {		
		    		for(MediaItem mediaItem : getMediaItemFromAlbumAttachment(attachment)) {
			    		mediaItems.add(mediaItem);
			    		mediaIds.add(mediaItem.getId());	
		    		}
		    	}
		    	else if(type.equals("article")) {		
		    		String link = attachment.getUrl();
					if (link != null) {
						WebPage webPage = new WebPage(link, id);
						webPage.setSource(source);
						webPages.add(webPage);
					}
					
					MediaItem mediaItem = getMediaItemFromArticleAttachment(attachment);
					if(mediaItem != null) {
			    		mediaItems.add(mediaItem);
			    		mediaIds.add(mediaItem.getId());	
		    		}	
		    	}
			}
		}
	
		List<String> urls = new ArrayList<String>();
		for(WebPage webPage : webPages) {
			try {
				urls.add(webPage.getUrl());
			}
			catch(Exception e) {
				continue;
			}
		}
		links = urls.toArray(new String[urls.size()]);
	}
	
	public GooglePlusItem(Activity activity, GooglePlusStreamUser user) {
		this(activity);
		
		//User that posted the post
		streamUser = user;
		if(user != null)
			uid = user.getId();
		
		
	}
	
	private List<MediaItem> getMediaItemFromAlbumAttachment (Attachments attachment) {
		List<MediaItem> mItems = new ArrayList<MediaItem>();
		
		for(Thumbnails thumbnail : attachment.getThumbnails()) {
			Activity.PlusObject.Attachments.Thumbnails.Image image = thumbnail.getImage();
			if(image != null && image.getWidth() > 250 && image.getHeight() > 250) {
				URL mediaUrl = null;
    			try {
    				mediaUrl = new URL(image.getUrl());
    			} catch (MalformedURLException e3) {
	    			continue;
	    		}
    			
				MediaItem mediaItem = new MediaItem(mediaUrl);
				
				String mediaId = Source.GooglePlus + "#"+attachment.getId(); 
				
				//id
				mediaItem.setId(mediaId);
				
				//SocialNetwork Name
				mediaItem.setSource(source);
				
				//Reference
				mediaItem.setReference(id);
				
				//Type 
				mediaItem.setType("image");
				
				//Time of publication
				mediaItem.setPublicationTime(publicationTime);
				
				//Author
				if(streamUser != null) {
					mediaItem.setUserId(streamUser.getId());
					mediaItem.setUser(streamUser);
				}
				//PageUrl
				mediaItem.setPageUrl(pageUrl);
				
				//Thumbnail
				String thumbnailUrl = thumbnail.getUrl();
				mediaItem.setThumbnail(thumbnailUrl);
				
				//Title
				mediaItem.setTitle(title);
				
				//Description
				mediaItem.setDescription(attachment.getDisplayName());
				
				//Tags
				mediaItem.setTags(tags);
				
				//Popularity
				mediaItem.setLikes(likes);
				mediaItem.setShares(shares);
    			
				//Size
				Long width = image.getWidth();
        		Long height = image.getHeight();
        		if(width != null && height != null) {
        			mediaItem.setSize(width.intValue(), height.intValue());
        		}
        		mItems.add(mediaItem);
			}
		}
		return mItems;
	}
	
	private MediaItem getMediaItemFromVideoAttachment(Attachments attachment) {
		MediaItem mediaItem = null;
		
		if(attachment.getId() == null) {
			return mediaItem;
		}
		
		Image image = attachment.getImage();
		Embed embed = attachment.getEmbed();
		if(embed != null) {
    		String videoUrl = embed.getUrl();
    		
			URL mediaUrl = null;
    		try {	
    			mediaUrl = new URL(videoUrl);
    		} catch (MalformedURLException e) {
    			return mediaItem;
    		}
    		
    		//url
    		mediaItem = new MediaItem(mediaUrl);
    		
    		String mediaId = Source.GooglePlus + "#"+attachment.getId(); 
    		
    		//id
			mediaItem.setId(mediaId);
			
			//SocialNetwork Name
			mediaItem.setSource(source);
			
			//Reference
			mediaItem.setReference(id);
			
			//Type 
			mediaItem.setType("video");
			
			//Time of publication
			mediaItem.setPublicationTime(publicationTime);
			
			//Author
			if(streamUser != null) {
				mediaItem.setUser(streamUser);
				mediaItem.setUserId(streamUser.getId());
			}
			
			//PageUrl
			mediaItem.setPageUrl(pageUrl);
			
			//Thumbnail
			String thumbUrl = image.getUrl();
			mediaItem.setThumbnail(thumbUrl);
			
			//Title
			mediaItem.setTitle(attachment.getDisplayName());
			
			//Description
			mediaItem.setDescription(attachment.getDisplayName());
			
			//Tags
			mediaItem.setTags(tags);
			
			//Popularity
			mediaItem.setLikes(likes);
			mediaItem.setShares(shares);	
		}
		
		return mediaItem;
	}
	
	private MediaItem getMediaItemFromPhotoAttachment(Attachments attachment) {
		MediaItem mediaItem = null;
		
		if(attachment.getId() == null) {
			return null;
		}
		
		FullImage image = attachment.getFullImage();
		String imageUrl = image.getUrl();
		Image thumbnail = attachment.getImage();
		
		Integer width = image.getWidth().intValue();
		Integer height = image.getHeight().intValue();
		
		if(thumbnail != null && (width > 250 && height > 250)) {
			URL mediaUrl = null;
    		try {
    			mediaUrl = new URL(imageUrl);
    		} catch (MalformedURLException e2) {
    			return null;
    		}

			//url
			mediaItem = new MediaItem(mediaUrl);
			
			String mediaId = Source.GooglePlus + "#"+attachment.getId(); 
			
			//id
			mediaItem.setId(mediaId);
			
			//SocialNetwork Name
			mediaItem.setSource(source);
			
			//Reference
			mediaItem.setReference(id);
			
			//Type 
			mediaItem.setType("image");
			
			//Time of publication
			mediaItem.setPublicationTime(publicationTime);
			
			//Author
			if(streamUser != null) {
				mediaItem.setUserId(streamUser.getId());
				mediaItem.setUser(streamUser);
			}
			
			//PageUrl
			mediaItem.setPageUrl(pageUrl);
			
			//Thumbnail
			String thumnailUrl = thumbnail.getUrl();
			mediaItem.setThumbnail(thumnailUrl);
			
			//Title
			mediaItem.setTitle(attachment.getDisplayName());
			
			//Description
			mediaItem.setDescription(attachment.getDisplayName());
			
			//Tags
			mediaItem.setTags(tags);
			
			//Popularity
			mediaItem.setLikes(likes);
			mediaItem.setShares(shares);
			
			//Size
			mediaItem.setSize(width,height);
		}
		return mediaItem;
	}
	
	private MediaItem getMediaItemFromArticleAttachment(Attachments attachment) {
		MediaItem mediaItem = null;

		Image image = attachment.getImage();
		if (image != null) {
			
			FullImage fullImage = attachment.getFullImage();
			URL mediaUrl = null;
    		try {
    			if(fullImage != null)
    				mediaUrl = new URL(fullImage.getUrl());
    			else
    				mediaUrl = new URL(image.getUrl());
    		} catch (MalformedURLException e2) {
    			return null;
    		}
    		
			mediaItem = new MediaItem(mediaUrl);
			
			//id
			mediaItem.setId(id);
			
			mediaItem.setThumbnail(mediaUrl.toString());
			
			//Reference
			mediaItem.setReference(id);
			
			//Time of publication
			mediaItem.setPublicationTime(publicationTime);
			
			//Type 
			mediaItem.setType("image");
			
			//PageUrl
			mediaItem.setPageUrl(pageUrl);
			
			
			//Author
			if(streamUser != null) {
				mediaItem.setUserId(streamUser.getId());
				mediaItem.setUser(streamUser);
			}
			
			// set title
			mediaItem.setTitle(attachment.getDisplayName());
			
			mediaItem.setDescription(attachment.getContent());
			
			//Tags
			mediaItem.setTags(tags);
			
			//SocialNetwork Name
			mediaItem.setSource(source);
			
			if(image.getWidth() != null && image.getHeight() != null) {
				mediaItem.setSize(
						image.getWidth().intValue(), 
						image.getHeight().intValue()
					);
			}
			
		}
		
		return mediaItem;
	}
	
	public GooglePlusItem(Activity activity, StreamUser user) {
		this(activity);
		
		this.setStreamUser(user);
		this.uid = user.getId();
		
		for(MediaItem mItem : this.mediaItems) {
			mItem.setUserId(uid);
		}
	}
	
	public GooglePlusItem(Comment comment, Activity activity, GooglePlusStreamUser user) {
		
		if (comment == null) {
			return;
		}
		
		//Id
		id = Source.GooglePlus+"#"+comment.getId();
		
		//Reference to the original post
		reference = Source.GooglePlus+"#"+activity.getId();
		
		//SocialNetwork Name
		source = Source.GooglePlus.toString();
		
		//Timestamp of the creation of the post
		publicationTime = comment.getPublished().getValue();
		
		description = "Comment";
		//User that posted the post
		if(user != null) {
			streamUser = user;
			uid = streamUser.getId();
		}
		//Popularity of the post
		if(comment.getPlusoners() != null){
			likes = new Long(comment.getPlusoners().size());
		}
	}

}
