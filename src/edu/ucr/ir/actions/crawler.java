package edu.ucr.ir.actions;

import java.io.*;
import java.net.URL;
import java.util.HashMap;

import edu.ucr.ir.data.CrawlerData;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import edu.ucr.ir.data.*;

public class crawler {
    final static int MAX_DEPTH = 5;

    public static void do_crawl(String[] args)
    {
        // Root of crawler logic

        // This is one chuck of crawler data (one file)
        CrawlerData crawlerData = new CrawlerData();

        //TODO: Hashmap to track pages already crawled

        System.out.println("Starting crawl...");
        // Root or seed URLs to start crawling...
        String[] seedUrls = {
                "https://en.wikipedia.org/wiki/Criticism_of_The_Da_Vinci_Code",
        };

        CrawlerPageData pageData = new CrawlerPageData();
        for (String url: seedUrls)
        {
            crawlUrl(crawlerData, url, 0);
        }
    }

    private static void crawlUrl(CrawlerData crawlerData, String url, int Depth) {
        if (Depth > MAX_DEPTH) return;

        CrawlerPageData pageData = new CrawlerPageData();
        try {
            Document doc = Jsoup.connect(url).get();
            pageData.title = doc.title();
            pageData.body = doc.body().text();
            // Put more stuff here....

            // Add to data
            crawlerData.add_page(pageData);

            // Check if time to flush crawler data
            if (crawlerData.atSizeLimit())
            {
                crawlerData.writeJson();
                crawlerData.flush();
            }
        }
        catch(IOException ex) {
            System.out.println("Exception: " + ex.toString());
        }
    }

}
