package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class App {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "word count");
        job.setJarByClass(App.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);

        Job jobS = new Job(conf, "sort words");
        jobS.setJarByClass(App.class);
        jobS.setMapperClass(SortingMapper.class);
        jobS.setReducerClass(SortingReducer.class);
        jobS.setMapOutputKeyClass(CompositeKey.class);
        jobS.setMapOutputValueClass(CompositeKey.class);
        jobS.setOutputKeyClass(NullWritable.class);
        jobS.setSortComparatorClass(MyKeyComparator.class);
        jobS.setOutputValueClass(CompositeKey.class);
        FileInputFormat.addInputPath(jobS, new Path(args[1]));
        FileOutputFormat.setOutputPath(jobS, new Path(args[2]));
        System.exit(jobS.waitForCompletion(true) ? 0 : 1);
    }
}