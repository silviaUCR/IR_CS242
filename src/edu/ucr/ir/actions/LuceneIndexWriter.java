package edu.ucr.ir.actions;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

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

import edu.ucr.ir.data.CrawlerData;
import edu.ucr.ir.data.CrawlerPageData;
import edu.ucr.ir.data.JsonUtils;


public class LuceneIndexWriter {
    final int MAX_BODY_CHARS = 10000;

    String indexPath = null;
    String dataPath = null;
    IndexWriter indexWriter = null;

    public LuceneIndexWriter(String indexPath, String dataPath) {
        this.indexPath = indexPath != null ? indexPath : "C:\\Crawler Extract\\DaVinci_Index";
        this.dataPath = dataPath != null ? indexPath : "C:\\Crawler Extract\\DaVinci Code Wiki Page";
    }

    public void startIndexing() throws IOException {
        // Open the index writer

        if (!openIndex(this.indexPath))
        {
            System.out.println("Could not open index.");
            return;
        }

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
            System.out.println("Opening index at path: " + indexPath);
            FSDirectory dir = FSDirectory.open(Paths.get(indexPath));
            IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
            config.setOpenMode(OpenMode.CREATE);
            this.indexWriter = new IndexWriter(dir, config);
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
            this.addDocuments(page);
            System.out.println("Indexed page: " + page.title);
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