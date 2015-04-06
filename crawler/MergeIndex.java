package crawler;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import model.Page;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class MergeIndex {

	static final String mergedIndex = "anair3";
	
	public static void main(String[] args) {
		
		Node mergedNode = nodeBuilder().clusterName("ana").node();
		Client mergedClient = mergedNode.client();
		int count=1;	
		try {
			
			File urlFile = new File("index/esPages.txt");
			BufferedReader bufferedReader = new BufferedReader(new FileReader(urlFile));
			String docNo;
		
			while((docNo = bufferedReader.readLine()) != null) {
				try {
					SearchResponse response = mergedClient.prepareSearch("finalir3")
					        .setTypes("document")
					        .setQuery(QueryBuilders.matchQuery("docno", docNo))
					        .setFrom(0).setSize(1)
					        .execute()
					        .actionGet();
					
					SearchHit searchHitMyIndex = response.getHits().getHits()[0];
					
					Page page = new Page();
					
					page.setDocno(docNo);
					page.setHead((String)searchHitMyIndex.getSource().get("head"));
					page.setText((String)searchHitMyIndex.getSource().get("text"));
					page.setHttpResponse((String)searchHitMyIndex.getSource().get("httpResponse"));
					ArrayList<String> inLinks = (ArrayList<String>) searchHitMyIndex.getSource().get("inlinks");
					ArrayList<String> outLinks = (ArrayList<String>) searchHitMyIndex.getSource().get("outlinks");
				
					//System.out.println("Document : "+ docNo +"  ->  MyIndex -> " + searchHitMyIndex.getSource().get("inlinks"));
						
					SearchResponse responseMerged = mergedClient.prepareSearch(mergedIndex)
					        .setTypes("document")
					        .setQuery(QueryBuilders.matchQuery("docno", docNo))
					        .setFrom(0).setSize(1)
					        .execute()
					        .actionGet();
					if(responseMerged.getHits().getTotalHits()!=0) {
						SearchHit searchHitMergedIndex = responseMerged.getHits().getHits()[0];
						System.out.println("Document : "+ docNo +"  ->  MergedIndex");
	//					System.out.println("Now I am merging "+docNo+ " to that document mergerIndex");
						
						ArrayList<String> mergedIndexInLinks = (ArrayList<String>) searchHitMergedIndex.getSource().get("inlinks");
						
						HashSet<String> myInlinksSet = Sets.newHashSet(inLinks);
						for(String pageInIndex : mergedIndexInLinks) {
							myInlinksSet.add(pageInIndex);
						}
						ArrayList<String> updatedInlinks = Lists.newArrayList(myInlinksSet);
						
						XContentBuilder updateInlinks;
						updateInlinks = jsonBuilder().startObject()
						.field("script","ctx._source.inlinks = newlinks")
						.startObject("params")
							.field("newlinks", updatedInlinks)
						.endObject()
						.endObject();
						
						Crawler.client.prepareUpdate(mergedIndex, "document", docNo)
							.setSource(updateInlinks)
							.execute()
							.actionGet();	
						
					} else {
	//					System.out.println("There is no document in merged index : " + docNo );
						System.out.println("Now I am sending "+docNo+ " to mergerIndex");
						
						XContentBuilder builder;
						builder = jsonBuilder().startObject()
						.field("docno",page.getDocno())
						.field("head",page.getHead())
						.field("text",page.getText())
						.field("httpResponse",page.getHttpResponse())
						.field("outlinks",outLinks)
						.field("inlinks",inLinks)
						.endObject();
					
						Crawler.client.prepareIndex(mergedIndex, "document", docNo)
						.setSource(builder)
						.execute()
						.actionGet();
						
					}
					
					System.out.println(count++);
				} catch (ElasticsearchException e) {
					System.out.println("Elastic Search Exception inside while");
					e.printStackTrace();
				} catch (Exception e) {
					System.out.println("Exception on inside");
					e.printStackTrace();
				}
			}
			bufferedReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ElasticsearchException e) {
			System.out.println("Elastic Search Exception");
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		mergedClient.close();
		mergedNode.close();
		System.exit(0);
	}
	
	
	
}
