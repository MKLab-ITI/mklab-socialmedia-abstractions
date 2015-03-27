package gr.iti.mklab.framework.abstractions.socialmedia.items;

import java.util.Date;
import java.net.MalformedURLException;
import java.net.URL;

import org.jinstagram.entity.common.Caption;
import org.jinstagram.entity.common.ImageData;
import org.jinstagram.entity.common.Images;
import org.jinstagram.entity.users.feed.MediaFeedData;

import gr.iti.mklab.framework.abstractions.socialmedia.users.InstagramStreamUser;
import gr.iti.mklab.framework.common.domain.Item;
import gr.iti.mklab.framework.common.domain.Location;
import gr.iti.mklab.framework.common.domain.MediaItem;
import gr.iti.mklab.framework.common.domain.Source;

/**
 * Class that holds the information of a instagram image
 * @author ailiakop
 * @email  ailiakop@iti.gr
 */
public class InstagramItem extends Item {

	private static final long serialVersionUID = -8872330316768925229L;
	
	public InstagramItem() {
		
	}
	
	public InstagramItem(MediaFeedData image) throws MalformedURLException {
		
		if(image == null || image.getId() == null)
			return;
		
		//Id
		id = Source.Instagram + "#" + image.getId();
		//SocialNetwork Name
		source =  Source.Instagram.toString();
		//Timestamp of the creation of the photo
		int createdTime = Integer.parseInt(image.getCreatedTime());
		Date publicationDate = new Date((long) createdTime * 1000);
		publicationTime = publicationDate.getTime();
		//Title of the photo
		Caption caption = image.getCaption();
		String captionText = null;
		if(caption!=null){
			captionText = caption.getText();
			title = captionText;
		}
		//Tags
		int tIndex=0;
		int tagSize = image.getTags().size();
		tags = new String[tagSize];
		
		for(String tag:image.getTags())
			tags[tIndex++]=tag;	
		
		url = image.getLink();
		
		//User that posted the photo
        if(image.getUser() !=null){
                streamUser = new InstagramStreamUser(image.getUser());
                uid = streamUser.getId();
        }
		//Location
		if(image.getLocation() != null){
			double latitude = image.getLocation().getLatitude();
			double longitude = image.getLocation().getLongitude();
			
			location = new Location(latitude, longitude);
		}
		//Popularity
		likes = new Long(image.getLikes().getCount());
		
		//Getting the photo
		Images imageContent = image.getImages();
		ImageData thumb = imageContent.getThumbnail();
		String thumbnail = thumb.getImageUrl();
		
		ImageData standardUrl = imageContent.getStandardResolution();
		if(standardUrl != null){
			Integer width = standardUrl.getImageWidth();
			Integer height = standardUrl.getImageHeight();
		
			String url = standardUrl.getImageUrl();
		
			if(url!=null && (width>150) && (height>150)){
				URL mediaUrl = null;
				try {
					mediaUrl = new URL(url);
				} catch (MalformedURLException e) {
					
					e.printStackTrace();
				}
				
				//url
				MediaItem mediaItem = new MediaItem(mediaUrl);
				
				String mediaId = Source.Instagram + "#"+image.getId(); 
				
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
					mediaItem.setUser(streamUser);
					mediaItem.setUserId(streamUser.getId());
				}
				//PageUrl
				mediaItem.setPageUrl(image.getLink());
				//Thumbnail
				mediaItem.setThumbnail(thumbnail);
				//Title
				mediaItem.setTitle(title);
				//Description
				mediaItem.setDescription(description);
				//Tags
				mediaItem.setTags(tags);
				//Popularity
				mediaItem.setLikes(likes);
				mediaItem.setComments(new Long(image.getComments().getCount()));
				//Location
				mediaItem.setLocation(location);

				mediaItems.add(mediaItem);
				mediaIds.add(mediaId);
			
			}
			
		}

	}
	
	public InstagramItem(MediaFeedData image,InstagramStreamUser user) throws MalformedURLException {
		this(image);
		
		//User that posted the post
		streamUser = user;
		uid = streamUser.getId();
	
		for(MediaItem mi : this.getMediaItems()) {
			mi.setUserId(uid);
		}
	}
	
}
