package edu.ucr.ir.data;

import java.io.File;
import java.io.IOException;

import java.lang.Math;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import  com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;

public class CrawlerData {
    // Constants
    final int MAX_SIZE_MB = 100;

    // This is our class that represents crawler data we wish to write as a JSON file
    ArrayList<CrawlerPageData> pages = new ArrayList<CrawlerPageData>();
    int sizeBytes = 0;

    public void add_page(CrawlerPageData page)
    {
        this.pages.add((page));

        // Rough tracking of how big our data file is getting (bytes)
        this.sizeBytes += page.url.length() + page.title.length() + page.body.length() + page.images.size()*64 + page.links.size()*64;
    }

    public boolean atSizeLimit()
    {
        return (this.sizeBytes >= MAX_SIZE_MB * (Math.pow(1024,2)));
    }

    public void flush()
    {
        // Erase all data
        this.pages.clear();
        this.sizeBytes = 0;
    }

    // These functions write the data to a JSON file, there are two functions of same name, but different parameters
    // This is called overloading, it allows user to specify a filename or not
    public void writeJson()
    {
        // No filename, specified, generate one
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd-HHmmss");
        String generatedName = formatter.format(new Date());
        generatedName += ".json";
        try
        {
            writeJson(generatedName);
        }
        catch (IOException ex)
        {
            System.out.println("Error writing JSON: " + ex);
        }

    }

    public void writeJson(String filename) throws IOException {
        // Serialize and write our data
        ObjectMapper om = new ObjectMapper();
        om.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        om.writeValue(new File(filename), this);
        System.out.println("Wrote JSON to: " + filename);
    }
}
