package crawler;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.ArrayList;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.xcontent.XContentBuilder;

public class ElasticSearchInlinkThread implements Runnable {

	String url;
	ArrayList<String> inlinks;
	
	public ElasticSearchInlinkThread (String url, ArrayList<String> inlinks) {
		this.url = url;
		this.inlinks = new ArrayList<String>(inlinks);
	}
	
	public void run() {
		
		XContentBuilder updateInlinks;
		try {
			System.out.println("Inlink sent:" + url);
			updateInlinks = jsonBuilder().startObject()
			.field("script","ctx._source.inlinks = newlinks")
			.startObject("params")
				.field("newlinks", inlinks)
			.endObject()
			.endObject();
			
			Crawler.client.prepareUpdate(Crawler.indexName, "document", url)
				.setSource(updateInlinks)
				.execute()
				.actionGet();
			
		} catch (IOException e) {
			System.out.println("IOException");
			e.printStackTrace();
			return;
		} catch (ElasticsearchException e) {
			System.out.println("Elastic Search Exception");
			e.printStackTrace();
			return;
		} catch (Exception e) {
			System.out.println("Error Exception");
			e.printStackTrace();
			return;
		}
		
	}
	
}
