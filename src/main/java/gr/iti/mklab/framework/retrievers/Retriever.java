package gr.iti.mklab.framework.retrievers;

import gr.iti.mklab.framework.common.domain.feeds.Feed;

public interface Retriever {
	
	/**
	 * Retrieves a feed that is inserted into the system (Feeds currently supported
	 * by the platform are: KeywordFeeds,LocationFeeds,SourceFeeds,ListFeeds,URLFeeds)
	 * @param feed
	 * @return
	 */
	public Response retrieve(Feed feed) throws Exception;
	
	public Response retrieve(Feed feed, Integer requests) throws Exception;
	
	/**
	 * Stops the retriever
	 * @param 
	 * @return
	 */
	public void stop();
}
