package wordcount;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Q3Mapper1 extends Mapper <Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
     
        String prev= new String("the");
        String next = new String();
        boolean flag = false;
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            String token = itr.nextToken();
            token = token.toLowerCase().replaceAll("[^a-z0-9]+","");
            if(flag)
            {
            word.set(token);
            context.write(word, one);
            flag=!flag;
            }
            if (token.equals(prev))
                flag=true;
                
        }
    }

}
