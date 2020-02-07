package edu.ucr.ir.actions;

import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;

import edu.ucr.ir.data.CrawlerData;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import edu.ucr.ir.data.*;
public class WebCrawler {
    static HashMap<String, Boolean> visitedUrls = new HashMap<String, Boolean>();
    static CrawlerData crawlerData = new CrawlerData();
    static String dataFolder = "";
    static String seedUrl = "";

    static int maxCrawlDepth = 3;

    // Helper class to count/time things, will autostart the timer
    static PerformanceStats perf = new PerformanceStats();

    public static void do_crawl(String outputFolder, String seed, int crawlDepth)
    {
        maxCrawlDepth = crawlDepth;
        if (maxCrawlDepth <= 0)
            maxCrawlDepth = 3; //some sane default
        dataFolder = outputFolder;
        seedUrl = seed;
        System.out.println("Starting crawl...");
        CrawlerPageData pageData = new CrawlerPageData();
        perf.reset();
        crawlUrl(seedUrl, 0);
        // Write last chunk of crawler data
        if (crawlerData.pages.size() > 0)
            crawlerData.writeJson(outputFolder);
        System.out.println("Completed. " + perf.getString());
    }

    static void crawlUrl(String url, int Depth) {

        //Return if we're over our max crawl depth
        if (Depth >= maxCrawlDepth) return;

        // Return if we've already visited this page
        if (visitedUrls.containsKey(url)) return;
        visitedUrls.put(url,true);
        System.out.println("("+ crawlerData.pageCount + ")" + url + "...");
        CrawlerPageData pageData = new CrawlerPageData();
        try {
            // Get the document
            //Document doc = Jsoup.connect(url).get();
            Document doc = SSLHelper.getConnection(url).get();

            // Parse out url
            pageData.url = url;

            // Parse out page title and body text
            pageData.title = doc.title();
            pageData.body = doc.body().text();

            // Metadata
            String metaDescription = "";
            try {
                metaDescription = doc.select("meta[name=description]").get(0).attr("content");
            }
            catch (Exception e) {}

            String metaKeywords = "";
            try {
                metaKeywords = doc.select("meta[name=keywords]").first().attr("content");
            }
            catch (Exception e) {}

            pageData.metaDescription = metaDescription;
            pageData.metaKeywords = metaKeywords;

            // Parse out image data
           /* pageData.images = new ArrayList<String>();
            Elements images = doc.select("img[src~=(?i)\\.(png|jpe?g|gif)]");
            for (Element image: images)
            {
                String imageData = image.attr("src") + ";" + image.attr("alt");
                pageData.images.add(imageData);
            }*/

            // Parse links
            pageData.links = new ArrayList<String>();
            Elements links = doc.select("a[href]");
            for (Element link: links)
            {
                pageData.links.add(link.attr("abs:href"));
            }

            // Add to data
            crawlerData.add_page(pageData);
            perf.count();

            // Check if time to flush crawler data
            if (crawlerData.atSizeLimit())
            {
                crawlerData.writeJson(dataFolder);
                crawlerData.flush();
            }

            // Crawl links in the page
            for (String childPage: pageData.links)
            {
                crawlUrl(childPage, (Depth + 1));
            }
        }
        catch(IOException ex) {
            System.out.println("Exception: " + ex.toString());
        }
    }

}
