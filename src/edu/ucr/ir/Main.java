package edu.ucr.ir;

// Command-line libs we need
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

// Our packages
import edu.ucr.ir.actions.*;

public class Main {
    public static void main(String[] args) throws Exception {
        /*
            Entry-point of application
         */
        System.out.println("Starting...");

        // Sample command line for web crawler
        //String[] testArgs = {"-c","-oc","C:\\Crawler Extract\\DaVinci Code Wiki Page\\","-cd","4","-s","https://en.wikipedia.org/wiki/Kobe_Bryant","https://en.wikipedia.org/wiki/Mazda_RX-7"};

        // Sample command line for indexing
        String[] testArgs = {"-iw","-oc","C:\\Crawler Extract\\DaVinci Code Wiki Page\\","-oi","C:\\Crawler Extract\\DaVinci_Index\\"};

        CommandLine results = parseArguments(testArgs);

        //Manually call these here to test without worrying about the CLI stuff.
        //LuceneIndexWriter liwTest = new LuceneIndexWriter(null,null);
        //liwTest.startIndexing();


        // Crawler
        if (results.hasOption("c"))
        {
            String[] seedUrls = {};
            // Output folder
            String outputFolder = results.getOptionValue("oc","C:\\IR242_Data");
            // Crawler depth
            int crawlDepth = 3;
            crawlDepth = Integer.parseInt(results.getOptionValue("cd"));
            // Seed URLS
            String seedUrl = "";
            if (results.hasOption("s"))
                seedUrl = results.getOptionValue("s","https://en.wikipedia.org/wiki/Apache_Lucene");
            WebCrawler.do_crawl(outputFolder, seedUrl, crawlDepth);
        }

        // IndexWriter
        if (results.hasOption("iw"))
        {
            String indexFolder = results.getOptionValue("oi","C:\\IR242_Index");
            String crawlData = results.getOptionValue("oc","C:\\IR242_Data");
            LuceneIndexWriter liw = new LuceneIndexWriter(indexFolder, crawlData);
            liw.startIndexing();
        }

        // IndexReader
        if (results.hasOption("ir"))
        {
            String indexFolder = results.getOptionValue("oi","C:\\IR242_Index");
            String searchTerm =  results.getOptionValue("t","no-search-term-defined");
            LuceneIndexReader.doSearch(indexFolder, searchTerm);
        }

        // Help
        if (results.hasOption('h'))
            printHelp();
    }

    private static Options buildOptions()
    {
        var options = new Options();
        options.addOption("c", "crawl", false, "Crawl Mode");
        options.addOption("h", "help", false, "Show Help");
        options.addOption("ir", "indexRead", false, "Index Read Mode");
        options.addOption("iw", "indexWrite", false, "Index Write Mode");
        options.addOption("s", "seed", true, "Seed Url");
        options.addOption("t", "term", true, "Search Term");
        options.addOption("cd", "crawlDepth", true, "Max crawl depth (default 5)");
        options.addOption("oc", "crawlData", true, "Crawler Output Folder");
        options.addOption("oi", "indexData", true, "Index Folder");
        return options;
    }

    private static CommandLine parseArguments(String[] args) {
        // Sets up the command-line argument parser
        CommandLineParser parser = new DefaultParser();

        // Try to parse
        try {
            var cliOptions = buildOptions();
            return parser.parse(cliOptions, args);
        } catch (ParseException ex) {
            System.err.println("Error during parse: " + ex.toString());
            return null;
        }
    }

    private static void printHelp() {
        // Print list of cli options to console
        var formatter = new HelpFormatter();
        formatter.printHelp("IR_CS242", buildOptions(), true);
    }
}
