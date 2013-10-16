package eu.socialsensor.framework.abstractions.dailymotion;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import eu.socialsensor.framework.common.domain.Location;
import eu.socialsensor.framework.common.domain.MediaItem;

/**
 * Class that holds the information regarding the dailymotion video
 * @author manosetro
 * @email  manosetro@iti.gr
 *
 */
public class DailyMotionMediaItem extends MediaItem {

	public DailyMotionMediaItem(DailyMotionVideo video) throws Exception {
		super(new URL(video.embed_url));
		
		this.setId("Dailymotion::" + video.id);
		this.setTitle(video.title);
		this.setTags(video.tags);
		this.setPageUrl(video.url);
		this.setThumbnail(video.thumbnail_url);
		
		this.setPublicationTime(1000 * video.created_time);
		
		this.setType("video");
		this.setStreamId("Dailymotion");
		
		// Set popularity
		Map<String, Integer> popularity = new HashMap<String, Integer>();
		popularity.put("views", video.views_total);
		popularity.put("rating", video.rating);
		popularity.put("ratings", video.ratings_total);
		popularity.put("comments", video.comments_total);
		this.setPopularity(popularity);
		
		double[] geoloc = video.geoloc;
		if(geoloc != null && geoloc.length>0) {
			Location location = new Location(geoloc[0], geoloc[1]);
			this.setLocation(location);
		}
	}

}
