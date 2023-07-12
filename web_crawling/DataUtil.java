package crawler_hw2;


import java.util.ArrayList;

public class DataUtil {
	ArrayList<FetchedUrl> fetchedUrls;
	ArrayList<VisitedUrl> visitedUrls;
	ArrayList<ExtractedUrl> extractedUrls;
	
	public DataUtil() {
		fetchedUrls = new ArrayList<FetchedUrl>();
		visitedUrls = new ArrayList<VisitedUrl>();
		extractedUrls = new ArrayList<ExtractedUrl>();
	}
	
	public void addFetchedUrls(String url, int statusCode) {
		this.fetchedUrls.add(new FetchedUrl(url, statusCode));
	}
	
	public void addExtractedUrls(String url, String residenceIndicator) {
		this.extractedUrls.add(new ExtractedUrl(url, residenceIndicator));
	}
	
	public void addVisitedUrls(String url, int size, int noOfOutlinks, String contentType) {
		this.visitedUrls.add(new VisitedUrl(url, size, noOfOutlinks, contentType));
	}
	

}

class VisitedUrl{
	String url;
	int size;
	int numberOfOutlinks;
	String contentType;
	
	public VisitedUrl(String url, int size, int noOfOutlinks, String contentType) {
		this.url = url;
		this.size = size;
		this.numberOfOutlinks = noOfOutlinks;
		this.contentType = contentType;
	}
}

class FetchedUrl {
	String url;
	int statusCode;
	
	public FetchedUrl(String url, int statusCode) {
		this.url = url;
		this.statusCode = statusCode;
	}
}


class ExtractedUrl {
	String url;
	String isWithinSite;
	
	public ExtractedUrl(String url, String isWithinSite) {
		this.url = url;
		this.isWithinSite = isWithinSite;
	}
}

