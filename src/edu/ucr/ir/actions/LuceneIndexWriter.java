package edu.ucr.ir.actions;

import java.io.*;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.FSDirectory;


import edu.ucr.ir.data.*;

public class LuceneIndexWriter {
    final int MAX_BODY_CHARS = 10000;

    String indexPath = null;
    String dataPath = null;
    StringBuilder sbStats = new StringBuilder();
    IndexWriter indexWriter = null;
    PerformanceStats perf = new PerformanceStats();

    public LuceneIndexWriter(String indexPath, String dataPath) {
        this.indexPath = indexPath != null ? indexPath : "C:\\IR242_Index";
        this.dataPath = dataPath != null ? dataPath : "C:\\IR242_Data";
    }

    public void startIndexing() throws IOException {
        // Init the stringbuilder of stats
        sbStats.setLength(0); //Effectively erases buffer
        sbStats.append("elapsedMS,pageCount" + System.lineSeparator());

        // Open the index writer
        if (!openIndex(indexPath))
        {
            System.out.println("Could not open index.");
            return;
        }

        // Get list of files in our data folder
        perf.reset();
        File[] dataFiles = listFiles(dataPath);

        // Iterate over each file and add them to index
        for (File file: dataFiles) {
            System.out.println("Parsing JSON file: " + file.getName());
            parseJson(file.getAbsolutePath());
        }
        sbStats.append(perf.getCSV() + System.lineSeparator()); //one last capture

        // Commit and close the index
        closeIndex();
        System.out.println("Indexing complete. " + perf.getString());

        // Print the stats
        System.out.println(sbStats.toString());
    }

    public Boolean openIndex(String indexPath){
        try {
            //Begining of StopWord Filter
            String token1 = "";
            Scanner inFile1 = new Scanner(new File("StopWordList.txt")).useDelimiter("\r\n");
            // Using ArrayList
            List<String> listStopWords = new ArrayList<String>();
            // Read each line in the text file into a list
            while (inFile1.hasNext()) {
                listStopWords.add(inFile1.next());
            }
            inFile1.close();

            //Convert to string array to pass to stopwords
            CharArraySet stopSet = StopFilter.makeStopSet(listStopWords.toArray(new String[0]));
            //End of StopWord Filter Remove stopSet from Analyzer to undo

            System.out.println("Opening index at path: " + indexPath);
            FSDirectory dir = FSDirectory.open(Paths.get(indexPath));

            IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer(stopSet));
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
            long count = perf.count();

            // Every 1-second, let's capture stats
            if (perf.peekLapMilli() >= 1000)
            {
                sbStats.append(perf.getCSV() + System.lineSeparator());
                perf.getLapMilli(); //Reset lap timer
            }
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
            doc.add(new TextField("body", pageData.body.substring(0,this.MAX_BODY_CHARS), Field.Store.YES));
        else
            doc.add(new TextField("body", pageData.body, Field.Store.YES));
/*
        for (String img: pageData.images)
            doc.add(new TextField("image", img, Field.Store.YES));
        for (String link: pageData.links)
            doc.add(new TextField("image", link, Field.Store.YES));*/
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