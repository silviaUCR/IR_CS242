package inverted_index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

public class CustomRecordReader extends RecordReader<Text, Text> {

    private final int NLINESTOPROCESS = 1;
    private long start;
    private long end;
    private long pos;
    private LineReader in;
    private int maxLineLength;
    private Text key = new Text();
    private Text value = new Text();

    //private static final Log LOG =
    //LogFactory.getLog(CustomRecordReader.class);

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {


        // FileInputSplit
        FileSplit split = (FileSplit) genericSplit;
        // Retrieve configuration
        // bytes
        Configuration job = context.getConfiguration();
        this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);

        // start from "start" and "end" positions
        start = split.getStart();
        end = start + split.getLength();

        final Path file = split.getPath();
        FileSystem fs = file.getFileSystem(job);
        FSDataInputStream fileIn = fs.open(split.getPath());

        boolean skipFirstLine = false;
        if (start != 0) {
            skipFirstLine = true;
            // Set the file pointer at "start - 1" position.
            --start;
            fileIn.seek(start);
        }

        in = new LineReader(fileIn, job);

        // If first line needs to be skipped, read first line
        if (skipFirstLine) {
            Text dummy = new Text();
            // Reset "start" to "start + line offset"
            start += in.readLine(dummy, 0, (int) Math.min((long)Integer.MAX_VALUE, end - start));
        }

        // Position is the actual start
        this.pos = start;
    }


    @Override
    public boolean nextKeyValue() throws IOException {
        // Current offset is the key
        key.set(Long.toString(pos));
        int newSize = 0;

        if (value == null) {
            value = new Text();
        }
        if (key == null) {
            key = new Text();
        }
        //key.clear();
        //final Text endline = new Text();

        //for (int i = 0; i < NLINESTOPROCESS; i++){

        while (pos < end) {

            newSize = in.readLine(value, maxLineLength, Math.max((int) Math.min(Integer.MAX_VALUE, end - pos), maxLineLength));

            if (newSize == 0) {
                break;
            }
            pos += newSize;
            if (newSize < maxLineLength) {
                break;
            }
        }
        //}


        if (newSize == 0) {
            // We've reached end of Split
            key = null;
            value = null;
            return false;
        }
        else {
            // Tell Hadoop a new line has been found
            return true;
        }
    }

    /**
     * This methods are used by the framework to give generated key/value pairs
     * to an implementation of Mapper. Be sure to reuse the objects returned by
     * these methods if at all possible!
     */
    @Override
    public Text getCurrentKey() throws IOException,
            InterruptedException {
        return key;
    }

    /**
     * This methods are used by the framework to give generated key/value pairs
     * to an implementation of Mapper. Be sure to reuse the objects returned by
     * these methods if at all possible!
     */
    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    /**
     * Like the corresponding method of the InputFormat class, this is an
     * optional method used by the framework for metrics gathering.
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (start == end) {
            return 0.0f;
        }
        else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }

    /**
     * This method is used by the framework for cleanup after there are no more
     * key/value pairs to process.
     */
    @Override
    public void close() throws IOException {
        if (in != null) {
            in.close();
        }
    }
}
