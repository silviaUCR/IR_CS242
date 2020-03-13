//package edu.ucr.ir.actions;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.text.DateFormatSymbols;
import java.text.NumberFormat;
import java.math.RoundingMode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class TemperatureAgg {




//////////////////////////////////// START CUSTOM INPUT FORMAT ////////////////////////////////////////////////////////////

    public static class CustomInputFormat
            extends FileInputFormat<Text, Text> {
        @Override
        public RecordReader<Text, Text> createRecordReader(InputSplit split,
                                                           TaskAttemptContext context) throws IOException, InterruptedException {
            return new CustomRecordReader();
        }
    }



    public static class CustomRecordReader extends RecordReader<Text, Text> {

        private final int NLINESTOPROCESS = 1;
        private long start;
        private long end;
        private long pos;
        private LineReader in;
        private int maxLineLength;
        private Text key = new Text();
        private Text value = new Text();

        private static final Log LOG =
                LogFactory.getLog(CustomRecordReader.class);

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
//////////////////////////////////// END CUSTOM INPUT FORMAT ////////////////////////////////////////////////////////////

//////////////////////////////////// JOB 1 ////////////////////////////////////////////////////////////

    public static class MapTemp
            extends Mapper<Text, Text, Text, Text>{
        private Text stn = new Text();
        private Text temp = new Text();
        public static enum CustomCounter {
            MEAS_CNT,
            MISSING_TEMP,
            MISSING_CNT,
        }
        private static final String fileTag = "TEMP~";
        private static final String MR_DATA_SEPARATOR = "\t";
        //private static final String DATA_SEPARATOR = " ";
        public void map(Text key, Text value, Context context
        ) throws IOException, InterruptedException {

            String line = value.toString();
            String stnno = null;
            //this if section skips blank/null rows and rows that have less than 5 chars.
            if (line == null || line.length() <= 0 || line.matches("(.*)EM(.*)")) { //ignores records that are null, zero length and are a header.
                stnno = "_";

            } else if (line.length() < 27) { //throws out records without temperature readings.
                stnno = "_";
            } else if (line.length() < 29) { //There are strings with numbers without decimals and counts
                stnno = line.substring(0, 6);
                String tp = line.substring(26,28);
                String mo = line.substring(18,20);
                double tmp = Double.parseDouble(tp.trim());
                int measct = 1;
                double temptotal = tmp;

                stn.set(stnno + MR_DATA_SEPARATOR + mo);  //creates the key
                temp.set(fileTag + temptotal + MR_DATA_SEPARATOR + measct);

                context.getCounter(CustomCounter.MISSING_CNT).increment(measct);
                context.write(stn, temp);

            } else if (line.length() <= 30) { //There are strings without counts

                stnno = line.substring(0, 6);
                String tp = line.substring(26,30);
                String mo = line.substring(18,20);
                double tmp = Double.parseDouble(tp.trim());
                int measct = 1;
                double temptotal = tmp;

                stn.set(stnno + MR_DATA_SEPARATOR + mo);  //creates the key
                temp.set(fileTag + temptotal + MR_DATA_SEPARATOR + measct);

                context.getCounter(CustomCounter.MISSING_CNT).increment(measct);
                context.write(stn, temp);


            } else { //This is to grab everything else

                stnno = line.substring(0, 6);
                String ct = line.substring(31,33);
                String tp = line.substring(26,30);
                String mo = line.substring(18,20);

                double tmp = Double.parseDouble(tp.trim());
                int measct = Integer.parseInt(ct.trim());
                double temptotal = tmp*measct;

                stn.set(stnno + MR_DATA_SEPARATOR + mo);  //creates the key
                temp.set(fileTag + temptotal + MR_DATA_SEPARATOR + measct);

                context.getCounter(CustomCounter.MEAS_CNT).increment(measct);
                context.write(stn, temp);

            }


        }

    }


    public static class MapState
            extends Mapper<Text, Text, Text, Text>{
        public static enum CustomCounter {
            STN_CNT
        }
        private static final String fileTag = "STN~";
        private static final String MR_DATA_SEPARATOR = "\t";
        private static final String DATA_SEPARATOR = ",";
        private Text stndetail = new Text();
        private Text stn = new Text();
        public void map(Text key, Text value, Context context
        ) throws IOException, InterruptedException {

            String line = value.toString();

            StringTokenizer st = new StringTokenizer(line,DATA_SEPARATOR);
            String tokens[] = line.split(DATA_SEPARATOR);

            if (st.countTokens() <= 10) { //some records have commas in the 3rd field. This sorts them out.
                String stnno = tokens[0].substring(1, tokens[0].length()-1);
                String ctry = tokens[3].substring(1, tokens[3].length()-1);
                String state = tokens[4].substring(1, tokens[4].length()-1);
                if (ctry != null && ctry != "" && ctry != " ") { //Filters out records without a country
                    if (ctry.contains("US")) { //Filters out records for US only
                        if (state != null && !state.isEmpty() && Character.isLetter(state.charAt(0))){ //Filters out records without states

                            stndetail.set(fileTag + ctry + MR_DATA_SEPARATOR + state);
                            for (int i = 1; i <= 12; i++) {
                                stn.set(stnno + MR_DATA_SEPARATOR + String.format("%02d",i));
                                context.getCounter(CustomCounter.STN_CNT).increment(1);
                                context.write(stn, stndetail);
                            }


                        }
                    }
                }

            } else { //Filters out records without a country
                String stnno = tokens[0].substring(1, tokens[0].length()-1);
                String ctry = tokens[4].substring(1, tokens[4].length()-1);
                String state = tokens[5].substring(1, tokens[5].length()-1);
                if (ctry != null && ctry != "" && ctry != " ") { //Filters out records without a country
                    if (ctry.contains("US")) { //Filters out records for US only
                        if (state != null && !state.isEmpty() && Character.isLetter(state.charAt(0))){ //Filters out records without states
                            stndetail.set(fileTag + ctry + MR_DATA_SEPARATOR + state);
                            for (int i = 1; i <= 12; i++) {
                                stn.set(stnno + MR_DATA_SEPARATOR + String.format("%02d",i));
                                context.getCounter(CustomCounter.STN_CNT).increment(1);
                                context.write(stn, stndetail);
                            }


                        }
                    }
                }
            }


        }
    }


    public static class StateTempJoin
            extends Reducer<Text, Text, Text, Text> {
        public static final String TAG_SEPARATOR = "~";
        private static final String MR_DATA_SEPARATOR = "\t";
        @Override
        public void reduce(Text key, Iterable<Text> values,
                           Reducer<Text, Text, Text, Text>.Context context
        ) throws IOException, InterruptedException {

            String value;
            String[] month;
            String[] splittedValues;
            String tag;
            String data = null, tempDetails = null;
            float temp = 0;
            float count = 0;
            String state = null, datakey = null, mo = null;


            for (Text txtValue : values) {
                month = key.toString().split(MR_DATA_SEPARATOR);
                value = txtValue.toString();
                splittedValues = value.split(TAG_SEPARATOR);
                tag = splittedValues[0];
                mo = month[1];
                if (tag.equalsIgnoreCase("TEMP")) {
                    String tokens[] = splittedValues[1].split(MR_DATA_SEPARATOR);

                    temp += Float.parseFloat(tokens[0]);
                    count += Float.parseFloat(tokens[1]);

                    tempDetails = temp + MR_DATA_SEPARATOR + count;


                } else if (tag.equalsIgnoreCase("STN")) {
                    String vals[] = splittedValues[1].split(MR_DATA_SEPARATOR);
                    state = vals[1];
                }
            }

            //&& result == Math.min(result)

            if (tempDetails != null && state != null ) {
                datakey = state + MR_DATA_SEPARATOR + mo;
                data = tempDetails;
                context.write(new Text(datakey), new Text(data));

            }



        }
    }
//////////////////////////////////// END JOB 1 ////////////////////////////////////////////////////////////

//////////////////////////////////// JOB 2 ////////////////////////////////////////////////////////////

    public static class MapJoinedData
            extends Mapper<Text, Text, Text, Text>{
        private Text statemo = new Text();
        private Text tempct = new Text();
        private static final String MR_DATA_SEPARATOR = "\t";
        public static enum CustomCounter {
            RES_CNT,

        }
        public void map(Text key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split(MR_DATA_SEPARATOR);
            String state = tokens[0];
            String month = tokens[1];
            String temp = tokens[2];
            String measct = tokens[3];
            statemo.set(state + MR_DATA_SEPARATOR + month);
            tempct.set (temp + MR_DATA_SEPARATOR + measct);

            context.write(statemo,tempct);
        }
    }


    public static class StateMonthReduce
            extends Reducer<Text, Text, Text, Text> {
        private Text output = new Text();
        private static final String MR_DATA_SEPARATOR = "\t";
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            String value;
            String[] tokens;
            float temp = 0;
            float count = 0;
            float result = 0;

            for (Text txtValue : values) {
                value = txtValue.toString();
                tokens = value.split(MR_DATA_SEPARATOR);
                temp += Float.parseFloat(tokens[0]);
                count += Float.parseFloat(tokens[1]);
            }

            result = temp/count;
            output.set(result + MR_DATA_SEPARATOR + count);

            context.write(key,output);
        }



    }

//////////////////////////////////// END JOB 2 ////////////////////////////////////////////////////////////

//////////////////////////////////// JOB 3 ////////////////////////////////////////////////////////////

    public static class MapAverage
            extends Mapper<Text, Text, Text, Text>{
        private Text state = new Text();
        private Text tempct = new Text();
        private static final String MR_DATA_SEPARATOR = "\t";
        public static enum CustomCounter {
            RES_CNT,

        }
        public void map(Text key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split(MR_DATA_SEPARATOR);
            String st = tokens[0];
            String month = tokens[1];
            String temp = tokens[2];
            String measct = tokens[3];
            state.set(st);
            tempct.set (temp + MR_DATA_SEPARATOR + measct + MR_DATA_SEPARATOR + month);

            context.write(state,tempct);
        }
    }


    public static class MaxMinReduce
            extends Reducer<Text, Text, Text, Text> {
        private Text output = new Text();
        private static final String MR_DATA_SEPARATOR = "\t";

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            String value;
            String[] tokens;
            double temp = 0;
            double mintemp = 0;
            double count = 0;
            double diff = 0;
            double min = 3000;
            double max = 0;
            int mo = 0;
            String month = null;
            String maxmonth = null;
            String minmonth = null;

            for (Text txtValue : values) {



                value = txtValue.toString();
                tokens = value.split(MR_DATA_SEPARATOR);
                temp = Double.parseDouble(tokens[0]);
                count += Double.parseDouble(tokens[1]);
                mo = Integer.parseInt(tokens[2]);
                month = new DateFormatSymbols().getMonths()[mo-1];

                if (temp > max) {
                    max = temp;
                    maxmonth = month;

                } else if (temp < min) {
                    min = temp;
                    minmonth = month;
                }

                diff = Math.abs(max-min);

            }


            output.set(diff + MR_DATA_SEPARATOR + max + MR_DATA_SEPARATOR + maxmonth + MR_DATA_SEPARATOR + min + MR_DATA_SEPARATOR + minmonth + MR_DATA_SEPARATOR + count);
            context.write(key,output);
        }



    }



//////////////////////////////////// END JOB 3 ////////////////////////////////////////////////////////////


//////////////////////////////////// JOB 4 ////////////////////////////////////////////////////////////

    public static class SortMaxMin
            extends Mapper<Text, Text, Text, Text>{
        private Text outputkey = new Text();
        private Text outputvalue = new Text();
        private static final String MR_DATA_SEPARATOR = "\t";
        public static enum CustomCounter {
            RES_CNT,

        }
        public void map(Text key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split(MR_DATA_SEPARATOR);
            String state = tokens[0];

            double diff = Double.parseDouble(tokens[1]);
            double max = Double.parseDouble(tokens[2]);
            double min = Double.parseDouble(tokens[4]);
            double count = Double.parseDouble(tokens[6]);
            ///////// IMPORTANT FOR SORT /////
            NumberFormat formatter = NumberFormat.getInstance();
            formatter.setMinimumIntegerDigits(2);
            String finaldiff = formatter.format(diff);
            ///////// Important for output display /////
            //formatter.setMinimumIntegerDigits(0);
            //formatter.setMaximumFractionDigits(0);
            //formatter.setRoundingMode(RoundingMode.UP);
            //String diffvalue = formatter.format(diff);
            //String maxvalue = formatter.format(max);
            //String minvalue = formatter.format(min);
            //String countvalue = formatter.format(count);


            String maxmonth = tokens[3];
            String minmonth = tokens[5];

            outputkey.set(finaldiff + MR_DATA_SEPARATOR + state);
            outputvalue.set (diff + MR_DATA_SEPARATOR + max + MR_DATA_SEPARATOR + maxmonth + MR_DATA_SEPARATOR + min + MR_DATA_SEPARATOR + minmonth +MR_DATA_SEPARATOR + count);

            context.write(outputkey,outputvalue);
        }
    }


    public static class ReduceSort
            extends Reducer<Text, Text, Text, Text> {
        private Text outputkey = new Text();
        private Text outputvalue = new Text();
        private static final String MR_DATA_SEPARATOR = "\t";

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            String state = null;
            String diff = null;
            String max = null;
            String maxmonth = null;
            String min = null;
            String minmonth = null;
            String count = null;

            for (Text txtValue : values) {
                String line = txtValue.toString();
                String keyline = key.toString();
                String[] tokens = line.split(MR_DATA_SEPARATOR);
                String[] keys = keyline.split(MR_DATA_SEPARATOR);

                state = keys[1];
                diff = tokens[0];
                max = tokens[1];
                maxmonth = tokens[2];
                min = tokens[3];
                minmonth = tokens[4];
                count = tokens[5];
            }

            outputkey.set(state);
            outputvalue.set("DIFF: " + diff + MR_DATA_SEPARATOR +"MAX:" + max + "," + maxmonth + MR_DATA_SEPARATOR + "MIN:" + min + "," + minmonth + MR_DATA_SEPARATOR +  "CNT: " + count);

            context.write(outputkey,outputvalue);




        }



    }



//////////////////////////////////// END JOB 4 ////////////////////////////////////////////////////////////


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Path out = new Path(args[2]);

        Job job = Job.getInstance(conf, "temp agg join");
        job.setJarByClass(TemperatureAgg.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), CustomInputFormat.class, MapTemp.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), CustomInputFormat.class, MapState.class);
        job.setReducerClass(StateTempJoin.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(out, "out1"));
        if (!job.waitForCompletion(true)) {
            System.exit(1);
        }

        //--------START CHAIN MAP REDUCE JOB(2)---------------------//

        Job job2 = Job.getInstance(conf, "temp agg average");
        job2.setJarByClass(TemperatureAgg.class);
        MultipleInputs.addInputPath(job2, new Path(out, "out1"), CustomInputFormat.class, MapJoinedData.class);
        job2.setReducerClass(StateMonthReduce.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job2, new Path(out, "out2"));

        if (!job2.waitForCompletion(true)) {
            System.exit(1);
        }

        //--------START CHAIN MAP REDUCE JOB(3)---------------------//


        Job job3 = Job.getInstance(conf, "temp agg final");
        job3.setJarByClass(TemperatureAgg.class);
        MultipleInputs.addInputPath(job3, new Path(out, "out2"), CustomInputFormat.class, MapAverage.class);
        job3.setReducerClass(MaxMinReduce.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job3, new Path(out, "out3"));

        if (!job3.waitForCompletion(true)) {
            System.exit(1);
        }

        //--------START CHAIN MAP REDUCE JOB(4)---------------------//

        Job job4 = Job.getInstance(conf, "temp agg sort");
        job4.setJarByClass(TemperatureAgg.class);
        //MultipleInputs.addInputPath(job4, new Path(args[0]), CustomInputFormat.class, SortMaxMin.class); //TESTING PURPOSES
        MultipleInputs.addInputPath(job4, new Path(out, "out3"), CustomInputFormat.class, SortMaxMin.class);
        job4.setReducerClass(ReduceSort.class);
        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(Text.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job4, new Path(out, "out4"));

        if (!job4.waitForCompletion(true)) {
            System.exit(1);
        }

    }
}





