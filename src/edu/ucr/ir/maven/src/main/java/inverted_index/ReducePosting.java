package inverted_index;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducePosting
        extends Reducer<Text, Text, Text, Text> {
    public static final String TAG_SEPARATOR = "~";
    private static final String MR_DATA_SEPARATOR = "\t";
    @Override
    public void reduce(Text key, Iterable<Text> values,
                       Reducer<Text, Text, Text, Text>.Context context
    ) throws IOException, InterruptedException {

        String value;
        //String[] month;
        String[] splittedValues;
        //String tag;
        String word_url = null;
        String datakey = null;
        String posting = null;
        int count = 0;

        for (Text txtValue : values) {
            word_url = key.toString();
            count += 1;
        }

        posting = Integer.toString(count);
        datakey = word_url;
        context.write(new Text(datakey), new Text(posting));

    }



}
