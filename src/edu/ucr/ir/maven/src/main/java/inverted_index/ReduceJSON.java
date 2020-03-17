package inverted_index;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

public class ReduceInvertedIndex extends
        Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        String jsonLine = "";

        for (Text text:values){
            jsonLine += text;
        }

        context.write(new Text(jsonLine), new Text(""));
    }
}