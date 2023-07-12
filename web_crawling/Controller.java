package crawler_hw2;


import edu.uci.ics.crawler4j.crawler.*;
import edu.uci.ics.crawler4j.fetcher.*;
import edu.uci.ics.crawler4j.robotstxt.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

public class Controller {
	
	static String crawlStorageFolder = "/Users/siddhantbagga/eclipse-workspace/crawler_hw5/data/crawl";
	static String webSiteUrl = "https://www.latimes.com/";
	static int numberOfCrawlers = 7;
	static String webSiteName = "latimes";
	static DataUtil dataUtil;
	
	
	public static void main(String[] args) throws Exception {
		
		dataUtil = new DataUtil();
		
		int maximumPagesToFetch = 20000;
		int maximumDepthOfCrawling = 16;
		int politenessDelay = 200;
		
		CrawlConfig config = new CrawlConfig();
		
		config.setCrawlStorageFolder(crawlStorageFolder);
		config.setMaxPagesToFetch(maximumPagesToFetch);
		config.setMaxDepthOfCrawling(maximumDepthOfCrawling);
		config.setPolitenessDelay(politenessDelay);
		config.setIncludeBinaryContentInCrawling(true);
		
		PageFetcher pageFetcher = new PageFetcher(config);
		RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
		RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher);
		CrawlController controller = new CrawlController(config, pageFetcher, robotstxtServer);
		
		controller.addSeed(webSiteUrl);
		controller.start(MyCrawler.class, numberOfCrawlers);
		List<Object> alldataUtil =  controller.getCrawlersLocalData();
		
		
		for(Object iterator : alldataUtil) {
			DataUtil data = (DataUtil) iterator;
			dataUtil.fetchedUrls.addAll(data.fetchedUrls);
			dataUtil.visitedUrls.addAll(data.visitedUrls);
			dataUtil.extractedUrls.addAll(data.extractedUrls);
		}

		
		File fetchFile = new File("fetch_" + webSiteName + ".csv");
		fetchFile.delete();
		fetchFile.createNewFile();
		
		writeFetchCSV(fetchFile);
		
		File visitFile = new File("visit_" + webSiteName + ".csv");
		visitFile.delete();
		visitFile.createNewFile();
		
		writeVisitCSV(visitFile);
		
		File urlsFile = new File("urls_" + webSiteName + ".csv");
		urlsFile.delete();
		urlsFile.createNewFile();

		writeURLsCSV(urlsFile);
		
		aggregateStats();
		
	}

	
	private static void writeFetchCSV(File file) throws Exception {
		BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file, true));
		bufferedWriter.append("Fetched URL,Status Code\n");
		for(FetchedUrl fetchUrl : dataUtil.fetchedUrls) {
			bufferedWriter.append(fetchUrl.url + "," + fetchUrl.statusCode + "\n");
		}
		bufferedWriter.close();
	}
	
	private static void writeVisitCSV(File file) throws Exception {
		BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file, true));
		bufferedWriter.write("Visited (Downloaded) URL,Size (Bytes),Number of Outlinks,Content Type\n");
		for(VisitedUrl visitUrl : dataUtil.visitedUrls) {
			bufferedWriter.append(visitUrl.url + "," + visitUrl.size + "," + visitUrl.numberOfOutlinks + "," + visitUrl.contentType + "\n");
		}
		bufferedWriter.close();
	}
	
	private static void writeURLsCSV(File file) throws Exception {
		BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file, true));
		bufferedWriter.write("URL,Within News Site or Not (OK/N_OK)\n");
		for(ExtractedUrl extractedUrl : dataUtil.extractedUrls) {
			bufferedWriter.append(extractedUrl.url + "," + extractedUrl.isWithinSite + "\n");
		}
		bufferedWriter.close();
	}
	
	
	private static void aggregateStats() throws Exception {
		
		int totalFetchesAttempted = getTotalFetchesAttempted();
		
		HashMap<Integer, Integer> statusCodes = getStatusCodesMap();
		
		int totalFetchesSucceeded = getTotalFetchesSucceeded(statusCodes);
		int totalFetchesAbortedOrFailed = getTotalFetchesAbortedOrFailed(totalFetchesAttempted, totalFetchesSucceeded);
		
		int totalExtractedUrls = dataUtil.extractedUrls.size();
		
		HashSet<String> uniqueExtractedUrls = new HashSet<String>();
		
		int totalUniqueUrlsWithinSite = 0;
		
		for(ExtractedUrl extractedUrl : dataUtil.extractedUrls) {
			if (!uniqueExtractedUrls.contains(extractedUrl.url)) {
				if (extractedUrl.isWithinSite == "OK") {
					totalUniqueUrlsWithinSite = totalUniqueUrlsWithinSite + 1;
				}
				uniqueExtractedUrls.add(extractedUrl.url);
			}
		}
		
		int totalOutgoingLinks = findTotalOutgoingLinks();
		
		int totalUniqueUrls = getTotalUniqueUrls(uniqueExtractedUrls);
		int totalUniqueUrlsOutsideSite = getTotalUniqueUrlsOutsideSite(totalUniqueUrls, totalUniqueUrlsWithinSite);
		
		
		int lessThan1K = 0, lessThan10K = 0, lessThan100K = 0, lessThan1M = 0, greaterThan1M = 0;
		HashMap<String, Integer> contentTypes = new HashMap<String, Integer>();
		
		for (VisitedUrl visitedUrl : dataUtil.visitedUrls) {
			if (visitedUrl.size < 1024) {lessThan1K ++;}
			else if (visitedUrl.size < 10240) {lessThan10K ++;}
			else if (visitedUrl.size < 102400) {lessThan100K ++;}
			else if (visitedUrl.size < 1048576) {lessThan1M ++;}
			else {greaterThan1M ++;}
			
			if (contentTypes.containsKey(visitedUrl.contentType)) {
				contentTypes.put(visitedUrl.contentType, contentTypes.get(visitedUrl.contentType) + 1);
			}
			else {
				contentTypes.put(visitedUrl.contentType, 1);
			}
			
		}
		
		File newFile = new File("CrawlReport_" + webSiteName + ".txt");
		newFile.delete();
		newFile.createNewFile();
		BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(newFile, true));
		
		writeCrawlReportIntroduction(bufferedWriter);
		
		writeFetchStats(bufferedWriter, totalFetchesAttempted, totalFetchesSucceeded, totalFetchesAbortedOrFailed);
		
		writeOutgoingLinksStats(bufferedWriter, totalOutgoingLinks, totalUniqueUrls, totalUniqueUrlsWithinSite, totalUniqueUrlsOutsideSite);
		
		writeStatusCodesStats(bufferedWriter, statusCodes);
		
		writeFileSizesStats(bufferedWriter, lessThan1K, lessThan10K, lessThan100K, lessThan1M, greaterThan1M);
		
		writeContentTypesStats(bufferedWriter, contentTypes);
		
		bufferedWriter.close();
		
	}

	private static int getTotalFetchesAttempted() {
		return dataUtil.fetchedUrls.size();
	}
	
	private static int getTotalFetchesSucceeded(HashMap<Integer, Integer> statusCodesMap) {
		return statusCodesMap.get(200);	
	}
	
	private static int getTotalFetchesAbortedOrFailed(int totalFetchesAttempted, int totalFetchesSucceeded) {
		return totalFetchesAttempted - totalFetchesSucceeded;
	}
	
	private static int getTotalUniqueUrls(HashSet<String> uniqueExtractedUrlUrlsSet) {
		return uniqueExtractedUrlUrlsSet.size();
	}
	
	private static int getTotalUniqueUrlsOutsideSite(int totalUniqueUrls, int totalUniqueUrlsWithinSite) {
		return totalUniqueUrls - totalUniqueUrlsWithinSite;
	}
		
	private static HashMap<Integer, Integer> getStatusCodesMap() {
		HashMap<Integer, Integer> statusCodesMap = new HashMap<Integer, Integer>();
		for(FetchedUrl fetchedUrl : dataUtil.fetchedUrls) {
			if (statusCodesMap.containsKey(fetchedUrl.statusCode)) {
				int newVal = statusCodesMap.get(fetchedUrl.statusCode) + 1;
				statusCodesMap.put(fetchedUrl.statusCode, newVal);
			}
			else {
				statusCodesMap.put(fetchedUrl.statusCode, 1);
			}
		}
		return statusCodesMap;
	}
	
	private static int findTotalOutgoingLinks() {
		int totalOutgoingLinks = 0;
		int totalVisits = 0;
		for(VisitedUrl visitUrl : dataUtil.visitedUrls) {
			totalOutgoingLinks = totalOutgoingLinks + visitUrl.numberOfOutlinks;
			totalVisits = totalVisits + 1;
		}
		return totalOutgoingLinks;
	}

	private static void writeCrawlReportIntroduction(BufferedWriter bufferedWriter) throws Exception {
		bufferedWriter.write("Name: Siddhant Bagga\nUSC ID: 4495959903\n" + "News site crawled: " + webSiteName +".com\nNumber of threads: " + numberOfCrawlers + "\n\n");
	}
	
	private static void writeFetchStats(BufferedWriter bufferedWriter, int numberOfFetchesAttempted, int numberOfFetchesSucceeded, int numberOfFetchesAbortedOrFailed) throws Exception {
		bufferedWriter.write("Fetch Statistics\n================\n");
		bufferedWriter.write("# fetches attempted: " + numberOfFetchesAttempted + "\n# fetches succeeded: " + numberOfFetchesSucceeded +"\n# fetches failed or aborted: " + numberOfFetchesAbortedOrFailed + "\n\n");
	}
	
	private static void writeOutgoingLinksStats(BufferedWriter bufferedWriter, int numberOfOutgoingLinks, int numberOfUniqueUrls, int numberOfUniqueLinksWithinSite, int numberOfUniqueLinksOutsideSite) throws Exception {
		bufferedWriter.write("Outgoing URLs:\n==============\n");
		bufferedWriter.write("Total URLs extracted: " + numberOfOutgoingLinks + "\n# unique URLs extracted: " + numberOfUniqueUrls + "\n" + "# unique URLs within News Site: " + numberOfUniqueLinksWithinSite + "\n# unique URLs outside News Site: " + numberOfUniqueLinksOutsideSite + "\n\n");
	}
	
	private static void writeStatusCodesStats(BufferedWriter bufferedWriter, HashMap<Integer, Integer> statusCodesMap) throws Exception {
		bufferedWriter.write("Status Codes:\n=============\n");
		bufferedWriter.write("200 OK: " + statusCodesMap.get(200) + "\n");
		bufferedWriter.write("301 Moved Permanently: " + statusCodesMap.get(301) + "\n");
		bufferedWriter.write("302 Found: " + statusCodesMap.get(302) + "\n");
		bufferedWriter.write("307 Temporary Redirect: " + statusCodesMap.get(307) + "\n");
		bufferedWriter.write("404 Not Found: " + statusCodesMap.get(404) + "\n\n");
	}
	
	private static void writeFileSizesStats(BufferedWriter bufferedWriter, int lessThan1KB, int lessThan10KB, int lessThan100KB, int lessThan1MB, int moreThan1MB) throws Exception {
		bufferedWriter.write("File Sizes:\n===========\n");
		bufferedWriter.write("< 1KB: "+ lessThan1KB + "\n");
		bufferedWriter.write("1KB ~ <10KB: "+ lessThan10KB + "\n");
		bufferedWriter.write("10KB ~ <100KB: "+ lessThan100KB + "\n");
		bufferedWriter.write("100KB ~ <1MB: "+ lessThan1MB + "\n");
		bufferedWriter.write(">= 1MB: "+ moreThan1MB + "\n\n");
	}
	
	private static void writeContentTypesStats(BufferedWriter bufferedWriter, HashMap<String, Integer> contentTypes) throws Exception {
		bufferedWriter.write("Content Types:\n==============\n");
		for(String contentType : contentTypes.keySet()) {
			bufferedWriter.write(contentType + ": " + contentTypes.get(contentType) + "\n");
		}
	}

}