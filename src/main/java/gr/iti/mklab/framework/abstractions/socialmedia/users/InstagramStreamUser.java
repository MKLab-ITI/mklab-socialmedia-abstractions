package gr.iti.mklab.framework.abstractions.socialmedia.users;

import org.jinstagram.entity.common.User;
import org.jinstagram.entity.users.basicinfo.Counts;
import org.jinstagram.entity.users.basicinfo.UserInfo;
import org.jinstagram.entity.users.basicinfo.UserInfoData;
import org.jinstagram.entity.users.feed.UserFeedData;

import gr.iti.mklab.framework.common.domain.Source;
import gr.iti.mklab.framework.common.domain.StreamUser;

/**
 * Class that holds the information of an instagram user
 * @author ailiakop
 */
public class InstagramStreamUser extends StreamUser {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -186352302816765493L;

	public InstagramStreamUser(User user) {

		if (user == null) return;
		
		//Id
		id = Source.Instagram + "#" + user.getId();
		//The id of the user in the network
		userid = user.getId();
		//The name of the user
		name = user.getFullName();
		//The username of the user
		username = user.getUserName();
		//streamId
		source = Source.Instagram.toString();
		//The description of the user
		description = user.getBio();
		//Profile picture of the user
		profileImage = user.getProfilePictureUrl();
		//The link to the user's profile
		url = user.getWebsiteUrl();
		//The link to the user's profile
		pageUrl = "http://instagram.com/" + username;
	
	}
	
	public InstagramStreamUser(UserInfoData user) {

		if (user == null) {
			return;
		}
		
		//Id
		id = Source.Instagram + "#" + user.getId();
		//The id of the user in the network
		userid = user.getId();
		//The name of the user
		name = user.getFullName();
		//The username of the user
		username = user.getUsername();
		//streamId
		source = Source.Instagram.toString();
		//The description of the user
		description = user.getBio();
		//Profile picture of the user
		profileImage = user.getProfile_picture();
		//The link to the user's profile
		pageUrl = "http://instagram.com/" + username;
		
		Counts counts = user.getCounts();
		if(counts != null) {
			items = counts.getMedia();
			friends = (long) counts.getFollows();
			followers = (long) counts.getFollwed_by();
		}
	}

	public InstagramStreamUser(UserInfo userInfo) {
		this(userInfo.getData());
	}

	public InstagramStreamUser(UserFeedData userFeed) {

		if (userFeed == null) {
			return;
		}
		
		//Id
		id = Source.Instagram + "#" + userFeed.getId();
		//The id of the user in the network
		userid = userFeed.getId();
		//The name of the user
		name = userFeed.getFullName();
		//The username of the user
		username = userFeed.getUserName();
		//streamId
		source = Source.Instagram.toString();
		//The description of the user
		description = userFeed.getBio();
		//Profile picture of the user
		profileImage = userFeed.getProfilePictureUrl();
		//The link to the user's profile
		pageUrl = "http://instagram.com/" + username;

	}
}
