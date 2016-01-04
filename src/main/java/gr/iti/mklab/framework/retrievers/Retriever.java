package gr.iti.mklab.framework.retrievers;

import gr.iti.mklab.framework.common.domain.feeds.Feed;

public interface Retriever {
	
	public Response retrieve(Feed feed) throws Exception;
	
	public Response retrieve(Feed feed, Integer requests) throws Exception;
	
	public void stop();
}
