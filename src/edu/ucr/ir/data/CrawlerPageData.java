package edu.ucr.ir.data;

import java.util.ArrayList;
import java.util.Date;

public class CrawlerPageData {
    public String url;
    public String title;
    public String body;
    public ArrayList<String> links;
    public ArrayList<String> images;
    public Date indexedOn;

    public void add_link(String link) {
        this.links.add((link));
    }

    public void add_image(String image) {
        this.images.add(image);
    }

    public CrawlerPageData() {
        this.url = "";
    }

    public CrawlerPageData(String url) {
        // Constructor stub - need to specify url when page is created
        this.url = url;
    }

    public CrawlerPageData(String url, String body) {
        // Constructor stub - accept url and body
        this.url = url;
        this.body = body;
    }
}
