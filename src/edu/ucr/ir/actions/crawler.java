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

public class crawler {
    final static int MAX_DEPTH = 3; //was 5, seemed a little too intense


    static HashMap<String, Boolean> visitedUrls = new HashMap<String, Boolean>();
    static CrawlerData crawlerData = new CrawlerData();

    public static void do_crawl(String[] args)
    {
        // Root of crawler logic

        // This is one chuck of crawler data (one file)
        CrawlerData crawlerData = new CrawlerData();

        //TODO: Hashmap to track pages already crawled

        System.out.println("Starting crawl...");
        // Root or seed URLs to start crawling...
        //"https://en.wikipedia.org/wiki/Criticism_of_The_Da_Vinci_Code" - did this already
        String[] seedUrls = {
                "https://medium.com/@autumnturpin/surviving-grad-school-with-your-mental-health-intact-fcd8d3839d10",
                "https://en.wikipedia.org/wiki/Kobe_Bryant",
        };

        CrawlerPageData pageData = new CrawlerPageData();
        for (String url: seedUrls)
        {
            crawlUrl(url, 0);
        }
    }

    private static void crawlUrl(String url, int Depth) {

        //Return if we're over our max crawl depth
        if (Depth > MAX_DEPTH) return;

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

            // Parse out image data
            pageData.images = new ArrayList<String>();
            Elements images = doc.select("img[src~=(?i)\\.(png|jpe?g|gif)]");
            for (Element image: images)
            {
                String imageData = image.attr("src") + ";" + image.attr("alt");
                pageData.images.add(imageData);
            }

            // Parse links
            pageData.links = new ArrayList<String>();
            Elements links = doc.select("a[href]");
            for (Element link: links)
            {
                pageData.links.add(link.attr("abs:href"));
            }

            // Add to data
            crawlerData.add_page(pageData);

            // Check if time to flush crawler data
            if (crawlerData.atSizeLimit())
            {
                crawlerData.writeJson();
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
