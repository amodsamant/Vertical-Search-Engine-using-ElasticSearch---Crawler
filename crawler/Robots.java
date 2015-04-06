package crawler;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.BufferedHttpEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;

import crawlercommons.robots.BaseRobotRules;
import crawlercommons.robots.SimpleRobotRules;
import crawlercommons.robots.SimpleRobotRules.RobotRulesMode;
import crawlercommons.robots.SimpleRobotRulesParser;


public class Robots {

	
	public static boolean parseURLRobots(String url) throws Exception {
	
		CloseableHttpClient httpclient = HttpClients.createDefault();
		
		String USER_AGENT = "crawler";
		
		URL urlObj = new URL(url);
		String hostId = urlObj.getProtocol() + "://" + urlObj.getHost()
		                + (urlObj.getPort() > -1 ? ":" + urlObj.getPort() : "");
		Map<String, BaseRobotRules> robotsTxtRules = new HashMap<String, BaseRobotRules>();
		BaseRobotRules rules = robotsTxtRules.get(hostId);
		if (rules == null) {
		    HttpGet httpget = new HttpGet(hostId + "/robots.txt");
		    HttpContext context = new BasicHttpContext();
		    HttpResponse response = httpclient.execute(httpget, context);
		    if (response.getStatusLine() != null && response.getStatusLine().getStatusCode() == 404) {
		        rules = new SimpleRobotRules(RobotRulesMode.ALLOW_ALL);
		        // consume entity to deallocate connection
		        EntityUtils.consumeQuietly(((org.apache.http.HttpResponse) response).getEntity());
		    } else {
		        BufferedHttpEntity entity = new BufferedHttpEntity(response.getEntity());
		        SimpleRobotRulesParser robotParser = new SimpleRobotRulesParser();
		        rules = robotParser.parseContent(hostId, IOUtils.toByteArray(entity.getContent()),
		                "text/plain", USER_AGENT);
		    }
		    robotsTxtRules.put(hostId, rules);
		}
	
		boolean urlAllowed = rules.isAllowed(url);
		
		return urlAllowed;
	}
	
	
	
}
