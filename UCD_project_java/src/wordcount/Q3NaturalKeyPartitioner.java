package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;


public class Q3NaturalKeyPartitioner extends Partitioner<BiGram, NullWritable> {


    @Override
    public int getPartition(BiGram key, NullWritable value, int i) {
        System.out.println("NaturalKeyPartitioner");
        return key.getWd().hashCode()%i;
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
