package wordcount;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Q3Mapper2 extends Mapper <Object, Text, BiGram , NullWritable> {

 
    
    private NullWritable nullValue = NullWritable.get();
    private BiGram biGram = new BiGram();
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
         StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
        while (itr.hasMoreTokens()) {
            String line = itr.nextToken();
            StringTokenizer word = new StringTokenizer(line);
            String word1 = word.nextToken();
            int count = Integer.parseInt(word.nextToken());
            BiGram bg = new BiGram(word1,count);
            context.write(bg, nullValue);
            
        }
    }

}
