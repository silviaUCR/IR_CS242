package edu.ucr.ir.actions;

import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.HashMap;

import edu.ucr.ir.data.CrawlerData;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import edu.ucr.ir.data.*;

public class WebCrawlerMultiThread {
    // !!!!! WORK IN PROCESS - NOT READY FOR USE - (Brandon) !!!!!


    // Multi-threaded version of the web crawler
    static ThreadPoolExecutor executor = null;

    static HashMap<String, Boolean> visitedUrls = new HashMap<String, Boolean>();
    static CrawlerData crawlerData = new CrawlerData();
    static String dataFolder = "";
    static String seedUrl = "";

    static int maxCrawlDepth = 3;

    // Helper class to count/time things, will autostart the timer
    static PerformanceStats perf = new PerformanceStats();

    class CrawlTask implements Runnable
    {
        private String name;

        public CrawlTask(String s)
        {
            name = s;
        }

        // Task entrypoint
        public void run()
        {
            try
            {
                Thread.sleep(1000);
                System.out.println(name+" complete");
            }
            catch(InterruptedException e) {e.printStackTrace();}
        }
    }

    public void WebCrawlerMultiThread(int numThreads)
    {
        executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numThreads);
    }

    void addUrl(String url)
    {
        Runnable r = new CrawlTask("x");
        executor.submit(r);
    }

}

