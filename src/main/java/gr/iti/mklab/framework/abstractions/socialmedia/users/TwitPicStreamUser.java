package gr.iti.mklab.framework.abstractions.socialmedia.users;

import com.google.api.client.util.Key;

import gr.iti.mklab.framework.common.domain.StreamUser;

public class TwitPicStreamUser extends StreamUser {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2219372938038811930L;

	public TwitPicStreamUser(TwitPicUser user) {

		id = "Twitpic#"+user.id;
		description = user.bio;
		username = user.username;
		name = user.name;

		items = user.photo_count;
		location = user.location;

		//createdAt = user.timestamp;
		profileImage = user.avatar_url;

	}
	
	/**
	 * Class that holds the information regarding the twitpic image
	 * @author manosetro
	 * @email  manosetro@iti.gr
	 */
	public static class TwitPicUser {
		@Key
		public String id, username, name, bio, avatar_url, timestamp, location;
		@Key
		public int photo_count;
	}
}