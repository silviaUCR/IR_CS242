package inverted_index;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReduceInvertedIndex
        extends Reducer<Text, Text, Text, Text> {
    private Text value = new Text();
    private static final String MR_DATA_SEPARATOR = "\t";
    private static final String POSTING_DS = ",";
    public void reduce(Text key, Iterable<Text> values,
                       Context context
    ) throws IOException, InterruptedException {
        String posting = null;


        for (Text txtValue : values) {
            posting += txtValue.toString() + POSTING_DS;
        }

        if(posting.endsWith(POSTING_DS))
        {
            posting = posting.substring(0,posting.length() - 1);
        }

        value.set(posting);

        context.write(key,value);
    }



}
