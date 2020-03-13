package edu.ucr.ir;

public class MapReduceMain {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Path out = new Path(args[1]);

        Job job = Job.getInstance(conf, "create posting with tf");
        job.setJarByClass(MapReduce.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), MapReduce.CustomInputFormat.class, MapReduce.MapPosting.class);
        job.setReducerClass(MapReduce.ReducePosting.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(out, "out1"));
        if (!job.waitForCompletion(true)) {
            System.exit(1);
        }
/*
    //--------START CHAIN MAP REDUCE JOB(2)---------------------//

    Job job2 = Job.getInstance(conf, "create inverted index with tf");
    job2.setJarByClass(edu.ucr.ir.maven.src.main.java.inverted_index.MapReduce.class);
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
    job3.setJarByClass(edu.ucr.ir.maven.src.main.java.inverted_index.MapReduce.class);
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
    job4.setJarByClass(edu.ucr.ir.maven.src.main.java.inverted_index.MapReduce.class);
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
