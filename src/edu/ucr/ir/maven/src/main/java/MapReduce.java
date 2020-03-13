import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class MapReduce {




  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    // configuration should contain reference to your namenode
    FileSystem fs = FileSystem.get(new Configuration());
// true stands for recursively deleting the folder you gave
    Path out = new Path(args[1]);
    fs.delete(out, true);

    Job job = Job.getInstance(conf, "create posting with tf");
    job.setJarByClass(MapReduce.class);
    MultipleInputs.addInputPath(job, new Path(args[0]), inverted_index.CustomInputFormat.class, inverted_index.MapPosting.class);
    job.setReducerClass(inverted_index.ReducePosting.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileOutputFormat.setOutputPath(job, out);
    if (!job.waitForCompletion(true)) {
		System.exit(1);
    }
/*
    //--------START CHAIN MAP REDUCE JOB(2)---------------------//

    Job job2 = Job.getInstance(conf, "create inverted index with tf");
    job2.setJarByClass(MapReduce.class);
    MultipleInputs.addInputPath(job2, new Path(out, "out1"), CustomInputFormat.class, MapInvertedIndex.class);
    job2.setReducerClass(ReduceInvertedIndex.class);
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
    job3.setJarByClass(MapReduce.class);
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
    job4.setJarByClass(MapReduce.class);
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

	 */

  }
}
		



