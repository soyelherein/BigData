package wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q1JobControl {

public static void main(String[] args) throws Exception {
    if ( args.length != 2)
    {
        System.out.println("2 argument expected:Indir|Outdir");
    }
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, " word count ");
    job.setJarByClass(Q1JobControl.class);
    job.setMapperClass(Q1Mapper.class);
    //job.setCombinerClass(WordCountCombiner.class);
    job.setReducerClass(Q1Reducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args [0]));
    FileOutputFormat.setOutputPath(job, new Path(args [1]));
    boolean flag = job.waitForCompletion(true);
    if (flag)
    {   Counter rec = job.getCounters().findCounter(records.distWordCount);
        System.out.println("Number of distinct words :"+ rec.getValue());
        rec = job.getCounters().findCounter(records.lessThanFour);
        System.out.println("Number of words with occurance<4 :"+ rec.getValue());
        rec = job.getCounters().findCounter(records.wordsStartZ);
        System.out.println("Number of distinct words starting with Z/z :"+ rec.getValue());
        System.exit(1);
    }
}

}

