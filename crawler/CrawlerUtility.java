package crawler;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;

import model.Page;
import model.UrlData;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Utility Class for generic functions needed for the crawler
 * @author Amod Samant
 *
 */
public class CrawlerUtility {

	/**
	 * Function to canonicalize URL and get domain
	 */
	public static String[] canonicalizeURL(String url) {
		
		url = url.replaceAll("http://http://","http://");
		
		String [] canonicalURL = new String[2];
		
		try {
			URL canURL = new URL(url);
			String path = canURL.getPath();
			if(path.contains("//"))
				path = path.replaceAll("//", "/");
			canonicalURL[0] = canURL.getProtocol() + "://" + canURL.getHost().toString().toLowerCase();
			canonicalURL[1] =canonicalURL[0]+path;
		} catch (MalformedURLException e) {
			System.out.println("Error in Canonicalization: "+ url);
			return null;
		}
		return canonicalURL;
	}
	
	/**
	 *  Function to check domain access - Politeness Policy
	 */
	public static void accessDomainCheck(String domain) {
		
		if(Crawler.domainLastAccessTimeMap.containsKey(domain)) {
			
			long timeDiff = System.currentTimeMillis() - Crawler.domainLastAccessTimeMap.get(domain);
			if( timeDiff < 1000) {
				try {
					TimeUnit.MILLISECONDS.sleep(1000-timeDiff);
				} catch (InterruptedException e) {
					e.printStackTrace();
					return;
				}
			}
		}
		Crawler.domainLastAccessTimeMap.put(domain,System.currentTimeMillis());
	}
	
	
	/**
	 * Function to process seeds to obtain main terms and do a somewhat focussed crawl
	 */
	public static void processSeed() {
		
		// certain generic are only added. Can be replaced by stopwords
		Crawler.hashUselessWords.add("and");
		Crawler.hashUselessWords.add("facts");
		Crawler.hashUselessWords.add("act");
		Crawler.hashUselessWords.add("care");
		Crawler.hashUselessWords.add("protection");

		for (String seed : Crawler.seeds) {
			
			URL canURL;
			try {
				canURL = new URL(seed);
			
				String path = canURL.getPath();
				
				String [] splitSeed = path.split("/"); 
						
				if(splitSeed.length>1) {
					for(int i=splitSeed.length-1;i<splitSeed.length;i++) {
						
						String[] tempOp = splitSeed[i].split("_|-");
						
						for(int j=0;j<tempOp.length;j++) {
							if(!Crawler.hashUselessWords.contains(tempOp[j].toLowerCase())) {
								Crawler.seedWords.add(tempOp[j].toLowerCase());
							}
						}
					}
				}
			} catch (MalformedURLException e) {
				System.out.println("Malformed Seed");
			} catch (Exception e) {
				System.out.println("Another Exception");
			}
				
		}
	}
	
	/**
	 *  Function to process the URLs and create the page to be sent to elastic search. 
	 * @throws InterruptedException 
	 */
	public static boolean processURL(String pageUrl) {
		
		HashSet<String> retrievedLinks = Sets.newHashSet();
		Document document;
		Crawler.urlsVisited.add(pageUrl);
		try {
			document = Jsoup.connect(pageUrl).get();
					
		} catch (IOException e) {
			System.out.println("ERROR in retrieving page : " + pageUrl);
			return false;
		} catch (Exception e) {
			System.out.println("Another Exception" + pageUrl);
			return false;
		}
		String pageText = document.text().trim();
		boolean relevantPage=false;
		for(String word : Crawler.seedWords) {
			if(pageText.toLowerCase().contains(word)) {
				relevantPage = true;
				break;
			}	
		}
		
		if(!relevantPage)
			return false;
		
		StringBuffer processedPage = new StringBuffer(pageText);
		processPageText(processedPage);
		
		Page page = new Page();
		page.setDocno(pageUrl);
		page.setHead(document.title());
		page.setText(processedPage.toString());
		page.setHttpResponse(document.html());
		
		Crawler.urlInElasticSearch.add(pageUrl);
		
		UrlData urlData;
		ArrayList<String> inlinks;
		ArrayList<String> outlinks;
		
		if(Crawler.urlDataMap.containsKey(pageUrl)) {
			urlData = Crawler.urlDataMap.get(pageUrl);
			inlinks = urlData.getInlinks();
			outlinks = urlData.getOutlinks();
		} else {
			urlData = new UrlData();
			urlData.setUrl(pageUrl);
			inlinks = Lists.newArrayList();
			outlinks = Lists.newArrayList();
		}
		
		Elements links = document.select("a[href]");
		
		for (Element ahref : links) {
			
			String extractedLink = ahref.absUrl("href");

			if(extractedLink!=null && 
					!extractedLink.equalsIgnoreCase("") && 
					!(extractedLink.matches("[\\w].*\\.(jp|d|m|pp|o|s|c|pd|PD|D|S|t|b|x|ph)[A-Za-z0-9]+$"))) {
				
				String outlink = CrawlerUtility.canonicalizeURL(extractedLink)[1];
				if(outlink.equalsIgnoreCase(pageUrl)) 
					continue;
				
				if(!retrievedLinks.contains(outlink)) 
					retrievedLinks.add(outlink);
				else 
					continue;
				
				if(outlink==null || outlink.equalsIgnoreCase(""))
					System.out.println("Check This");
				
				outlinks.add(outlink);			
								
				UrlData outlinkObj;
				// Create URL Data Map object
				if(Crawler.urlDataMap.containsKey(outlink)) {
					outlinkObj = Crawler.urlDataMap.get(outlink);
					int inlinkCount = outlinkObj.getInlinksCount();
					outlinkObj.setInlinksCount(++inlinkCount);
					ArrayList<String> linkObjInlinks = outlinkObj.getInlinks();
					linkObjInlinks.add(pageUrl);
				} else  {
					outlinkObj = new UrlData();
					outlinkObj.setUrl(outlink);
					outlinkObj.setInlinksCount(1);
					ArrayList<String> linkObjInlinks = Lists.newArrayList();
					linkObjInlinks.add(pageUrl);
					outlinkObj.setInlinks(linkObjInlinks);
					outlinkObj.setOutlinks(new ArrayList<String>());
					
				}
				
				//Add to the queue if not added and visited
				if(!Crawler.urlsVisited.contains(outlink)) {
					
					if(!Crawler.linksMap.containsKey(outlink)) {
						outlinkObj.setTimeOfInsertion(System.currentTimeMillis());
						outlinkObj.setLevel(Crawler.linksMap.get(pageUrl).getLevel()+1);
					} else  {
						int inlinkCountTemp = outlinkObj.getInlinksCount();
						outlinkObj.setInlinksCount(++inlinkCountTemp);
						Crawler.linksQueue.remove(outlinkObj);
					}
					Crawler.linksQueue.add(outlinkObj);
					Crawler.linksMap.put(outlink,outlinkObj);
				}
				Crawler.urlDataMap.put(outlink,outlinkObj);
			}				
		}
		
		urlData.setInlinks(inlinks);
		urlData.setOutlinks(outlinks);
		
		Crawler.urlDataMap.put(pageUrl, urlData);
		Crawler.pages.add(page);	
		Crawler.count++;
		if(Crawler.pages.size()>Crawler.filesToWriteCount) {
			writeToIndex();
		}
		return true;
	}

	/**
	 * Function to write to index
	 * 
	 */
	public static void writeToIndex() {
		
		ElasticSearchThread newThread = new ElasticSearchThread(Crawler.pages, Crawler.urlDataMap);

		Crawler.executor.execute(newThread);
		
		// Clear the array
		Crawler.pages.clear();
		
	}
	
	/**
	 * Process page to match text with regular expression
	 */
	public static void processPageText(StringBuffer text) {
	
		Matcher token = Crawler.regexPattern.matcher(text);
		while(token.find()) {
			text.append(" ").append(token.group().toLowerCase());
		}
	}
	
	/**
	 *  Function to synchronize links between urldata map and elastic search
	 */
	public static void syncLinks() {
		
		for(Entry<String, UrlData> entry : Crawler.urlDataMap.entrySet()) {
			
			UrlData urlDataObj = entry.getValue();
			
			// Check if visited
			if(urlDataObj.isVisitedES()) {
				
				Crawler.inlinkGraphWriter.print(urlDataObj.getUrl());
				for(String inlink: urlDataObj.getInlinks()) {
					Crawler.inlinkGraphWriter.print("\t"+inlink);
				}
				Crawler.inlinkGraphWriter.println();	
				
				System.out.println("Written to file" + urlDataObj.getUrl() );
				
			
				ElasticSearchInlinkThread inlinkThread = new ElasticSearchInlinkThread(urlDataObj.getUrl(), urlDataObj.getInlinks());
				Crawler.inlinkExecutor.execute(inlinkThread);
				
				ArrayList<String> urlLinks = urlDataObj.getInlinks();
				urlLinks.clear();
			}
		}
	}
}