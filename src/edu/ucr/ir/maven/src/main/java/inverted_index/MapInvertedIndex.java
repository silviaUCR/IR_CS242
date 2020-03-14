package inverted_index;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapInvertedIndex
        extends Mapper<Text, Text, Text, Text> {
    private Text word = new Text();
    private Text url_tf = new Text();
    private static final String MR_DATA_SEPARATOR = "\t";
    public static enum CustomCounter {
        RES_CNT,

    }
    public void map(Text key, Text value, Context context
    ) throws IOException, InterruptedException {

        if(isNullOrEmpty(value.toString())) {

        } else {
            String word_url = key.toString();
            String[] tokens = word_url.split(MR_DATA_SEPARATOR);
            //String url = tokens[1];
            String tf = value.toString();
            word.set(tokens[0]);
            //url_tf.set (url + MR_DATA_SEPARATOR + tf);
            url_tf.set (tf);
            context.write(word,url_tf);
        }

    }

    public static boolean isNullOrEmpty(String str) {
        if(str != null && !str.isEmpty())
            return false;
        return true;
    }
}
