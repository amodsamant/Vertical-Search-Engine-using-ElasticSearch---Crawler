package crawler;
/**
 *  Main Class - Crawler
 *  @author Amod Samant
 */
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

import model.Page;
import model.UrlData;

import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class Crawler {
	
	static final int totalDocumentsToRetrieve = 12000;
	static final int filesToWriteCount = 400;
	
	static final String seed1 = "http://en.wikipedia.org/wiki/Barack_Obama";
	static final String seed2 = "http://obamacarefacts.com/obamacare-facts/";
	static final String seed3 = "http://en.wikipedia.org/wiki/Patient_Protection_and_Affordable_Care_Act";
	static final String seed4 = "http://www.britannica.com/EBchecked/topic/1673534/Patient-Protection-and-Affordable-Care-Act-PPACA";
	
	static ArrayList<String> seeds = Lists.newArrayList();
	static HashSet<String> seedWords = Sets.newHashSet();
	static HashSet<String> hashUselessWords = Sets.newHashSet();
		
	static PriorityQueue<UrlData> linksQueue = new PriorityQueue<UrlData>(new CustomFrontierComparator());
	static HashMap<String,UrlData> linksMap = Maps.newHashMap();
	
	static HashMap<String,Long> domainLastAccessTimeMap = Maps.newHashMap(); // key-domain value-time of domain access
	static HashMap<String,UrlData> urlDataMap = Maps.newHashMap();  // key - url string, value - url data
	static HashSet<String> urlsVisited = Sets.newHashSet(); // key - url
	
	static HashSet<String> urlInElasticSearch = Sets.newHashSet();
	
	static ArrayList<Page> pages = Lists.newArrayList();
	
	static ExecutorService executor = Executors.newCachedThreadPool();
	static ExecutorService inlinkExecutor = Executors.newCachedThreadPool();
	
	// Regular Expression Matching
	static Pattern regexPattern = Pattern.compile("\\w+(\\.?\\w+)*");
	
	static PrintWriter inlinkGraphWriter;
	static PrintWriter esPagesWriter;
			
	static int count = 0;
	static int id = 0;
	
	static int threadId = 0;
	
	static boolean done = false;
	
	static Node node = nodeBuilder().clusterName("ana").node();
	static Client client = node.client();
	
	static final String indexName = "newfinalir3";
	
	public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {

		long start = System.currentTimeMillis();
		
		// Adding seeds to the ArrayList
		seeds.add(seed1);
		seeds.add(seed2);
		seeds.add(seed3);
		seeds.add(seed4);
		CrawlerUtility.processSeed();
	
		for (String seed : seeds) {
			
			try {
				if (Robots.parseURLRobots(seed)) {
					String [] canonicalForm = CrawlerUtility.canonicalizeURL(seed);
					if(canonicalForm == null)
						continue;
					String seedDomain    = canonicalForm[0];
					String seedCanonical = canonicalForm[1];
					
					CrawlerUtility.accessDomainCheck(seedDomain);
					UrlData urlData;
					if(!Crawler.urlDataMap.containsKey(seedCanonical)) {
						
						urlData = new UrlData();
						urlData.setUrl(seedCanonical);
						urlData.setInlinks(new ArrayList<String>());
						urlData.setOutlinks(new ArrayList<String>());
						
						urlData.setInlinksCount(0);
						urlData.setLevel(0);
						
						Crawler.urlDataMap.put(seedCanonical,urlData);
						Crawler.linksMap.put(seedCanonical,urlData); 
					} else {
						urlData =  Crawler.urlDataMap.get(seedCanonical);
						urlData.setLevel(0);
						Crawler.urlDataMap.put(seedCanonical,urlData);
						Crawler.linksQueue.remove(urlData);
					}
					
					CrawlerUtility.processURL(seedCanonical);
					System.out.println("URL:           " + seedCanonical);
					System.out.println("Queue Size:    " + linksQueue.size());
				}
			} catch (Exception e) {
				System.out.println("Robots Parser Exception or someother exception in Seed URL");
				e.printStackTrace();
			}
		}
		
		while(!done) {
			
			if(linksQueue.isEmpty())
				break;
			
			UrlData frontierURL = linksQueue.poll();
			
			if(frontierURL==null)
				break;
			
			try {
				URL url = new URL(frontierURL.getUrl());
				
				URLConnection conn = url.openConnection();
				HttpURLConnection http = (HttpURLConnection)conn;
				int statusCode = http.getResponseCode();
				if(statusCode == 404) {
					Crawler.urlsVisited.add(frontierURL.getUrl());
					continue;
				}
			} catch (MalformedURLException e) {
				System.out.println("Malformed Exception"+frontierURL.getUrl());
				Crawler.urlsVisited.add(frontierURL.getUrl());
				continue;
			} catch (IOException e) {
				System.out.println("Cannot connect : "+frontierURL.getUrl());
				Crawler.urlsVisited.add(frontierURL.getUrl());
				continue;
			} catch (ClassCastException e) {
				System.out.println("ClassCastException : "+frontierURL.getUrl());
				Crawler.urlsVisited.add(frontierURL.getUrl());
				continue;			
			} catch (Exception e) {
				System.out.println("Another exception : "+frontierURL.getUrl());
				Crawler.urlsVisited.add(frontierURL.getUrl());
				continue;
			}
			
			try {
				if (frontierURL.getUrl()!=null && Robots.parseURLRobots(frontierURL.getUrl())) {	
					String [] canonicalForm = CrawlerUtility.canonicalizeURL(frontierURL.getUrl());
					
					if(canonicalForm==null)
						continue;
					String urlDomain = canonicalForm[0];
					String frontierCanonicalizedURL = canonicalForm[1];
					if(frontierCanonicalizedURL== null || frontierCanonicalizedURL.equalsIgnoreCase(""))
						continue;
						
					if(!urlDomain.equalsIgnoreCase("")) {
						CrawlerUtility.accessDomainCheck(urlDomain);
									
						if(!(frontierCanonicalizedURL.equalsIgnoreCase("")) && !Crawler.urlsVisited.contains(frontierCanonicalizedURL)) {
							boolean processedURL = CrawlerUtility.processURL(frontierCanonicalizedURL);
							
							if(processedURL) {
								System.out.println("URL:           " + frontierCanonicalizedURL);
								System.out.println("Page Count:    " + Crawler.count);
								if(count>=totalDocumentsToRetrieve) // Terminate after crawling 12000 frontier links
									done=true;
							}
						}
					}
				}
			} catch (Exception e) {
				System.out.println("Robots Parser Exception in Frontier");
				Crawler.urlsVisited.add(frontierURL.getUrl());
				continue;
			} 
		}
		
		
		CrawlerUtility.writeToIndex();
		
		Crawler.executor.shutdown();
		while(!Crawler.executor.isTerminated()) {
		}
		
		inlinkGraphWriter = new PrintWriter("index/inlinkGraph.txt", "UTF-8");
		
		CrawlerUtility.syncLinks();
		
		inlinkGraphWriter.flush();
		inlinkGraphWriter.close();
		
		Crawler.inlinkExecutor.shutdown();
		
		while(!Crawler.inlinkExecutor.isTerminated()) {
		}
		
		esPagesWriter = new PrintWriter("index/esPages.txt", "UTF-8");
		
		Iterator<String> esPageIterator = urlInElasticSearch.iterator();
		while(esPageIterator.hasNext()){
		    esPagesWriter.println(esPageIterator.next());
		}
		esPagesWriter.close();
		
		long end = System.currentTimeMillis();
		
		System.out.println("DONE in "+ (end-start)/60000 + " minutes");
		
		
		client.close();
		node.close();

	}
	
}