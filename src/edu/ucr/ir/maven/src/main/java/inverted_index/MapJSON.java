package inverted_index;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MapInvertedIndex
        extends Mapper<Text, Text, Text, Text> {
    private Text word_url_key = new Text();
    private Text value = new Text();

    public static enum CustomCounter {
        WORD_CNT,
        DOCUMENT_CNT,
    }

    private static final String fileTag = "INDEX~";
    private static final String MR_DATA_SEPARATOR = "\t";
    private static final String WORD_DS = " ";
    ArrayList<String> allWords;

    static List<String> stopwords;

    public void map(Text key, Text value, Context context
    ) throws IOException, InterruptedException {

        String jsonLine = value.toString();
        JsonElement jelement = new JsonParser().parse(jsonLine);
        JsonObject jobject = jelement.getAsJsonObject();
        stopwords = Files.readAllLines(Paths.get("stopword/StopWordList.txt"));
        word_url_key.set("1");  //creates the key
        value.set(jsonLine);  //creates the value. 1 is just a dummy variable
        context.write(word_url_key, value);

/*
        //ArrayList<String> urls = new ArrayList<String>(); TO STORE STRINGS - KEEP FOR LATER IF NEEDED
        JsonArray jarray = jobject.getAsJsonArray("pages");
        for (JsonElement urlHolder : jarray) {
            //urls.add(urlHolder.getAsJsonObject().getAsJsonPrimitive("url").getAsString()); // KEEP FOR LATER IF NEEDED
            String url = urlHolder.getAsJsonObject().getAsJsonPrimitive("url").getAsString();
            String body = urlHolder.getAsJsonObject().getAsJsonPrimitive("body").getAsString();
            //body = body.replaceAll("[^\\p{ASCII}]", "");
            //body = body.replaceAll("[^a-zA-Z0-9_-_ ]", "");

            //allWords.clear();

            allWords =
                    Stream.of(body.toLowerCase().split(" "))
                            .collect(Collectors.toCollection(ArrayList<String>::new));
            allWords.removeAll(stopwords);


            for (String word : allWords) {

                final Pattern p = Pattern.compile("\\W");
                final Matcher matcher = p.matcher(word);
                matcher.find();
                word = matcher.group(1);
                if(isNullOrEmpty(word)) {

                } else {
                    word_url_key.set(word);  //creates the key
                    value.set(url);  //creates the value. 1 is just a dummy variable
                    context.write(word_url_key, value);
                }

            }
        }*/
    }

    public static boolean isNullOrEmpty(String str) {
        if(str != null && !str.isEmpty())
            return false;
        return true;
    }


}
