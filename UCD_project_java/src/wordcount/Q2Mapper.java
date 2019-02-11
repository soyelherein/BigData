package wordcount;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class Q2Mapper extends Mapper <Object, Text, Text, Text> {
    
    private Text file = new Text();
    private Text word = new Text();
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
   
        String fileName = ((FileSplit)context.getInputSplit()).getPath().getName();
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            String token = itr.nextToken();
            token = token.toLowerCase().replaceAll("[^a-z0-9]+","");
            if(token.length()>0)
            {
            word.set(token);
            file.set(fileName);
            context.write(word, file);
            }
        }
    }

}
