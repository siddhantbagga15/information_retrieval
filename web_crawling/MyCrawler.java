package web_crawling;


import java.util.Set;
import java.util.regex.Pattern;
import edu.uci.ics.crawler4j.parser.*;
import edu.uci.ics.crawler4j.url.WebURL;
import edu.uci.ics.crawler4j.crawler.*;


public class MyCrawler extends WebCrawler {
	String WITHIN_SITE_STATUS = "OK";
	String OUTSIDE_SITE_STATUS = "N_OK";
	DataUtil dataUtil;
	String webSiteName = "latimes";
	String domain = "latimes.com";
	
	static String filterPattern = ".*(\\.(css|js|json|mp3|zip|gz|vcf|xml|mid|mp2|mp3|mp4|wav|avi|mov|mpeg|ram|m4v))$";
	private final static Pattern FILTERS = Pattern.compile(filterPattern);
	
	public MyCrawler() {
		dataUtil = new DataUtil();
	}
	
	@Override
	public boolean shouldVisit(Page page, WebURL webUrl) {
		String url = webUrl.getURL();
		
		if (url.endsWith("/")) {
			int urlSize = url.length();
			url = url.substring(0, urlSize - 1);
		}
			
		String lowerCaseUrl = url.toLowerCase();
		url = lowerCaseUrl.replace(",","_").replaceFirst("^(https?://)?(www.)?", "");
				
		if (!url.startsWith(domain)) {
			dataUtil.addExtractedUrls(webUrl.getURL(), OUTSIDE_SITE_STATUS);
		}
		else {
			dataUtil.addExtractedUrls(webUrl.getURL(), WITHIN_SITE_STATUS);
		}
		
		boolean result = url.startsWith(domain) && !FILTERS.matcher(url).matches();
				
		return result;
	}
	
	@Override
	public void handlePageStatusCode(WebURL url, int statusCode, String statusDescription) {	
		String finalUrl = url.getURL().replace(",","_");
		dataUtil.addFetchedUrls(finalUrl, statusCode);
	}
	
	@Override
	public void visit(Page page) {
		String contentType = getContentType(page);
		String url = page.getWebURL().getURL();
		if (checkIfHTMLContent(contentType)) {
			if (page.getParseData() instanceof HtmlParseData) {
				Set<WebURL> links = getAllOutLinks(page);				
				dataUtil.addVisitedUrls(url, page.getContentData().length, links.size(), contentType);
			}
		}
		else if (checkIfOtherContentTypes(contentType)) {
			dataUtil.addVisitedUrls(url, page.getContentData().length, 0, contentType);
		}
	}
	
	private static String getContentType(Page page) {
		return page.getContentType().toLowerCase().split(";")[0];
	}
	
	private static boolean checkIfHTMLContent(String contentType) {
		return contentType.equals("text/html");
	}
	
	private static boolean checkIfOtherContentTypes(String contentType) {
		return (contentType.equals("application/msword") || contentType.startsWith("image") 
				|| contentType.equals("application/pdf") || contentType.equals("application/document"));
	}
	
	private static Set<WebURL> getAllOutLinks(Page page) {
		HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();
		Set<WebURL> links = htmlParseData.getOutgoingUrls();	
		return links;
	}
	
	@Override
	public Object getMyLocalData() {
		return dataUtil;
	}
}