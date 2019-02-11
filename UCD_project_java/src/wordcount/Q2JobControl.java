package wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import wordcount.Q2Reducer;

public class Q2JobControl {

public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, " word count ");
    job.setJarByClass(Q2JobControl.class);
    job.setMapperClass(Q2Mapper.class);
    //job.setCombinerClass(WordCountCombiner.class);
    job.setReducerClass(Q2Reducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args [0]));
    FileOutputFormat.setOutputPath(job, new Path(args [1]));
    boolean flag = job.waitForCompletion(true);
    if (flag)
    {   Counter rec = job.getCounters().findCounter(recrd.wordInSingleDoc);
        System.out.println("Number of words in only one document :"+ rec.getValue());
        System.exit(1);
    }
}

}

