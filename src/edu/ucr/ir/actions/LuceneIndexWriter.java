package edu.ucr.ir.actions;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.util.List;
import java.util.Set;

import edu.ucr.ir.data.CrawlerPageData;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import edu.ucr.ir.data.CrawlerData;
import edu.ucr.ir.data.CrawlerPageData;
import edu.ucr.ir.data.JsonUtils;


public class LuceneIndexWriter {
    final int MAX_BODY_CHARS = 10000;

    String indexPath = null;
    String dataPath = null;
    IndexWriter indexWriter = null;

    public LuceneIndexWriter(String indexPath, String dataPath) {
        this.indexPath = indexPath != null ? indexPath : "C:\\IR242_Index";
        this.dataPath = dataPath != null ? indexPath : "C:\\IR242_Data";
    }

    public void startIndexing() throws IOException {
        // Open the index writer
        openIndex(this.indexPath);

        // Get list of files in our data folder
        File[] dataFiles = listFiles(this.dataPath);

        // Iterate over each file and add them to index
        for (File file: dataFiles) {
            System.out.println("Parsing JSON file: " + file.getName());
            parseJson(file.getAbsolutePath());
        }

        // Commit and close the index
        closeIndex();
    }

    public Boolean openIndex(String indexPath){
        try {
            Directory dir = FSDirectory.open(Paths.get(indexPath));
            Analyzer analyzer = new StandardAnalyzer();
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);

            //Always overwrite the directory
            iwc.setOpenMode(OpenMode.CREATE);
            System.out.println("Opening index at path: " + indexPath);
            this.indexWriter = new IndexWriter(dir, iwc);
            System.out.println("Indexed opened.");
            return true;
        } catch (Exception e) {
            System.err.println("Error opening the index. " + e.getMessage());
            return false;
        }
    }

    public void parseJson(String jsonFilePath) throws IOException {
        CrawlerData crawlerData = (CrawlerData) JsonUtils.readJsonFromFile(jsonFilePath, CrawlerData.class);
        for (CrawlerPageData page: crawlerData.pages) {
            addDocuments(page);
        }
    }

    /**
     * Add documents to the index
     */
    public void addDocuments(CrawlerPageData pageData){
        Document doc = new Document();
        doc.add(new StringField("url", pageData.url, Field.Store.YES));
        doc.add(new StringField("title", pageData.title, Field.Store.YES));

        // Lucene has a limit on amount of text to index
        if (pageData.body.length() > this.MAX_BODY_CHARS)
            doc.add(new StringField("body", pageData.body.substring(0,this.MAX_BODY_CHARS), Field.Store.YES));
        else
            doc.add(new StringField("body", pageData.body, Field.Store.YES));

        for (String img: pageData.images)
            doc.add(new TextField("image", img, Field.Store.YES));
        for (String link: pageData.links)
            doc.add(new TextField("image", link, Field.Store.YES));
        try {
            this.indexWriter.addDocument(doc);
        } catch (IOException ex) {
            System.err.println("Error adding documents to the index. " +  ex.getMessage());
        }
    }

    /**
     * Write the document to the index and close it
     */
    public void closeIndex() {
        try {
            System.out.println("Closing index...");
            this.indexWriter.commit();
            this.indexWriter.close();
            System.out.println("Index closed.");
        } catch (IOException ex) {
            System.err.println("We had a problem closing the index: " + ex.getMessage());
        }
    }

    public static File[] listFiles(String path)
    {
        File folder = new File(path);
        return folder.listFiles();
    }
}