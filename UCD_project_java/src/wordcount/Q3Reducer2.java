package wordcount;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

enum rec{
totWords;
};

public class Q3Reducer2 extends Reducer <BiGram, NullWritable, Text, IntWritable> {

    //private IntWritable result = new IntWritable();
    @Override
    protected void reduce(BiGram key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key.getWd(), key.getCnt());
        context.getCounter(rec.totWords).increment(1);
    }
    
    @Override
  public void run(Context context) throws IOException, InterruptedException {
    setup(context);
    int count = 0;
    while (context.nextKey()) {
        if (count++ < 5) {
        reduce(context.getCurrentKey(), context.getValues(), context);
        } else {
            break;
        }
    }
    cleanup(context);
  }

}
