package crawler;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import model.Page;
import model.UrlData;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.xcontent.XContentBuilder;

public class ElasticSearchThread implements Runnable {

	ArrayList<Page> pages;
	HashMap<String,UrlData> urlDataMap;
	
	public ElasticSearchThread(ArrayList<Page> pages, HashMap<String,UrlData> urlDataMap) {
		
		this.pages = new ArrayList<Page>(pages);
		this.urlDataMap = urlDataMap;
		
	}


	public void run() {
		
		System.out.println("Thread "+ ++Crawler.threadId +" running");
		
		for (Page p : pages) {

			UrlData urlData = urlDataMap.get(p.getDocno());
			XContentBuilder builder;
			try {
				builder = jsonBuilder().startObject()
				.field("docno",p.getDocno())
				.field("head",p.getHead())
				.field("text",p.getText())
				.field("httpResponse",p.getHttpResponse())
				.field("outlinks",urlData.getOutlinks())
				.endObject();
				
				//Inlinks written only at the end with separate thread
			
				Crawler.client.prepareIndex(Crawler.indexName, "document", p.getDocno())
				.setSource(builder)
				.execute()
				.actionGet();
				
				System.out.println("Sent to ES: "+ p.getDocno()+"\n");
				
				builder.close();
				
				urlData.getOutlinks().clear();
				urlData.setVisitedES(true);
				
			} catch (IOException e) {
				System.out.println("Error in Sending content to ES \n");
				e.printStackTrace();
				continue;
			} catch (ElasticsearchException e) {
				e.printStackTrace();
				continue;
			} catch (Exception e) {
				e.printStackTrace();
				continue;
			}
			
		}
		// Clear the array
		pages.clear();
		
	}	
}
