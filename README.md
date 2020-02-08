# IR_CS242
Homework and Project for Information Retrieval Class

#### Teams
* Silvia, Yue, Jasper, William, Brandon

### Project Modules
**Note:** Project is intended to be run as a command-line application
* WebCrawler - jSoup-based web crawler to obtain data we'll need for indexing
* LuceneIndexWriter - Lucene-based indexer to process the data that the crawler obtained
* LuceneIndexReader - Lucene-based index reader to query index

#### Using the CLI
`usage: IR_CS242 [-c] [-cd <arg>] [-h] [-ir] [-iw] [-oc <arg>] [-oi <arg>]`<br>
`       [-s <arg>] [-t <arg>]`<br>
` -c,--crawl               Crawl Mode`<br>
` -cd,--crawlDepth <arg>   Max crawl depth (default 5)`<br>
` -h,--help                Show Help`<br>
` -ir,--indexRead          Index Read Mode`<br>
` -iw,--indexWrite         Index Write Mode`<br>
` -oc,--crawlData <arg>    Crawler Output Folder`<br>
` -oi,--indexData <arg>    Index Folder`<br>
` -s,--seed <arg>          Seed Url`<br>
` -t,--term <arg>          Search Term`<br>
