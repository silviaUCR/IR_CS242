package inverted_index;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapPosting
        extends Mapper<Text, Text, Text, Text> {
    private Text word_url_key = new Text();
    private Text value = new Text();

    public static enum CustomCounter {
        WORD_CNT,
        DOCUMENT_CNT,
    }

    private static final String fileTag = "INDEX~";
    private static final String MR_DATA_SEPARATOR = "\t";
    private static final String WEBPAGE_DS = "\"url\":\"";
    private static final String BODY_DS = "\"body\":\"";
    private static final String LINK_DS = "\"links\":";
    private static final String WORD_DS = " ";

    public void map(Text key, Text value, Context context
    ) throws IOException, InterruptedException {
        JsonParser parser = new JsonParser();

        String json = value.toString();

        JsonElement jsonTree = parser.parse(json);

        if(jsonTree.isJsonObject()){
            JsonObject jsonObject = jsonTree.getAsJsonObject();

            JsonElement f1 = jsonObject.get("pages");

            if(f1.isJsonObject()){
                JsonObject f1Obj = f1.getAsJsonObject();

                JsonElement url = f1Obj.get("url");
                JsonElement body = f1Obj.get("body");
                word_url_key.set(String.valueOf(url));  //creates the key
                value.set("1");  //creates the value. 1 is just a dummy variable
                context.write(word_url_key, value);
            }
        }

    }


/*

			String line = value.toString();
			int end = 0;
			String webpages[] = line.split(WEBPAGE_DS);
			String url = null;

			for (String webpage : webpages){
				final Pattern pattern = Pattern.compile("\"url\":\"(.+?)\"", Pattern.DOTALL);
				final Matcher matcher = pattern.matcher("\"url\":\"" + webpage + "\"");
				matcher.find();
				// to catch any non url strings.
				if (matcher.group(1).contains("https://")) {
					url = matcher.group(1);
					word_url_key.set(url);  //creates the key
					value.set("1");  //creates the value. 1 is just a dummy variable
					context.write(word_url_key, value);

				}
				else {

				}

				String bodies[] = webpage.split(BODY_DS);

				for (String body : bodies) {
					final Pattern bodypattern = Pattern.compile("\"body\":\"(.+?)\"", Pattern.DOTALL);
					final Matcher bodymatcher = bodypattern.matcher("\"body\":" + body + ",\"metaDescription\":\"");
					bodymatcher.find();
					String body_dirty = bodymatcher.group(1);
					//String body_1[] = body.split(LINK_DS);
					word_url_key.set(body_dirty);  //creates the key
					value.set("1");  //creates the value. 1 is just a dummy variable
					context.write(word_url_key, value);
				}

				String[][] sp_chr_to_blk = {{",",""},{"\"",""},{"\\",""},{"\'",""},{":",""}}; //special characters to remove. could implement the same stop word list algo from part a.
				String body_clean = body_dirty;
				for(String[] replacement: sp_chr_to_blk) {
					body_clean = body_clean.replace(replacement[0], replacement[1]); //clean body after all the special characters are removed.
				}
				//String urls[] = webpage.split("\"");
				//String url_final = urls[0];
				//String words[] = body_clean.split(WORD_DS);
				/*
				for (String word : words){
					word_url_key.set(word + MR_DATA_SEPARATOR + url_final);  //creates the key
					value.set("1");  //creates the value. 1 is just a dummy variable
					context.write(word_url_key, value);
				}


			}

		}*/
}
