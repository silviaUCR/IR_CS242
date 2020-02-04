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
import edu.ucr.ir.data.*;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        /*
            Entry-point of application
         */
        System.out.println("Starting...");
        //indexer.indexCrawlerData("testfile.json");

        // Can pass arguments to command line here for debugging/running in IntelliJ
        String[] testArgs = {};
        //String[] testArgs = {"-c"};
        CommandLine results = parseArguments(testArgs);

        // Crawl?
        if (results.hasOption("c"))
            crawler.do_crawl(results.getArgs());

        // Index?
        if (results.hasOption('i'))
            indexer.main(results.getArgs());

        // Help
        if (results.hasOption('h'))
            printHelp();
    }

    private static Options buildOptions()
    {
        var options = new Options();
        options.addOption("c", "crawl", false, "Crawl Mode");
        options.addOption("h", "help", false, "Show Help");
        options.addOption("i", "index", false, "Index Mode");
        options.addOption("cd", "crawlDepth", true, "Max crawl depth (default 5)");
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
            //System.err.println(ex.toString());
            return null;
        }
    }

    private static void printHelp() {
        // Print list of cli options to console
        var formatter = new HelpFormatter();
        formatter.printHelp("IR_CS242", buildOptions(), true);
    }
}
