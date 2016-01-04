package gr.iti.mklab.framework.abstractions.socialmedia.users;

import com.google.api.services.plus.model.Activity.Actor;
import com.google.api.services.plus.model.Person;

import gr.iti.mklab.framework.common.domain.Source;
import gr.iti.mklab.framework.common.domain.StreamUser;

/**
 * Class that holds the information of a google plus user
 * @author ailiakop
 */
public class GooglePlusStreamUser extends StreamUser {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -2715125421888395821L;

	public GooglePlusStreamUser(Actor actor) {

		if (actor == null) return;
		
		//Id
		id = Source.GooglePlus + "#" + actor.getId();
		
		//The id of the user in the network
		userid = actor.getId();
		
		//The name of the user
		name = actor.getDisplayName();
		
		//The username of the user
		username = actor.getDisplayName();
		
		//streamId
		source = Source.GooglePlus.toString();
		
		//Profile picture of the user
		profileImage = actor.getImage().getUrl();
		
		//The link to the user's profile
		pageUrl = actor.getUrl();
		
		verified = false;
		
	}

	public GooglePlusStreamUser(Person person) {

		if (person == null) 
			return;
		
		//Id
		id = Source.GooglePlus + "#"+person.getId();
		
		//The id of the user in the network
		userid = person.getId();
		
		//The name of the user
		name = person.getDisplayName();
		
		//The username of the user
		username = person.getDisplayName();
		
		//The brief description of this person.
		description = person.getTagline();
		
		//source
		source = Source.GooglePlus.toString();
		
		//Profile picture of the user
		profileImage = person.getImage().getUrl();
		
		//The link to the user's profile
		pageUrl =  person.getUrl();
		
		verified = person.getVerified();
		
		if(person.getCircledByCount() != null) {
			followers = (long) person.getCircledByCount();
		}
		
		location = person.getCurrentLocation();

	}
	
	
}
