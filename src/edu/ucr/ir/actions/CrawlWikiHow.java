package edu.ucr.ir.actions;//import org.json.simple.JSONArray;
//import org.json.simple.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;



//    "https://www.wikihow.com/wikiHowTo?search=wifi+signal";

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class CrawlWikiHow {

    public static void saveToFile ( String file,String text,boolean append)
    {
        try {
            File f= new File(file);
            FileWriter fw= new FileWriter(f,append); // add the append true later
            PrintWriter pw= new PrintWriter(fw);
            pw.println(text);
            pw.close();
        } catch (IOException e) {
            System.out.println("Error: save to File");
        }



    }


    public static void main(String[] args) throws IOException {

        Document document=Jsoup.connect("https://www.wikihow.com/wikiHowTo?search=build+house").get();
        Elements ele=document.select("div#searchresults_list");
        for ( Element element:ele.select("div.result"))
        {
            // this first selection doesn't work for some reason:( )
            String url=element.select("result_link a").attr("href");
            System.out.println(url);

            String img=element.select("div.result_thumb img").attr("src");
            //System.out.println(img);
            saveToFile("buildhouse_file.txt",img,true);


            for (Element det:element.select("div.result_data"))

            { String title=det.select("div.result_title").text();
                //System.out.println(title);
                saveToFile("buildhouse_file.txt",title,true);
                String views=det.select("li.sr_view").text();
                //System.out.println(views);
                saveToFile("buildhouse_file.txt",views,true);
                String updated=det.select("li.sr_updated").text();
                //System.out.println(updated);
                saveToFile("buildhouse_file.txt",updated,true);
            }




        }



    }

}