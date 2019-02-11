package wordcount;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

enum records{
distWordCount,
wordsStartZ,
lessThanFour;
};

public class Q1Reducer extends Reducer <Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
            if (sum == 1)
            {   
                context.getCounter(records.distWordCount).increment(1);
                if (key.toString().length() > 0 && key.toString().charAt(0) == 'z' )
                {
                    context.getCounter(records.wordsStartZ).increment(1);
                }
            }
            
        }
        if(sum<4)
        {
            context.getCounter(records.lessThanFour).increment(1);
        }
        result.set(sum);
        context.write(key, result);
    }

}
