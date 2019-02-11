package wordcount;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.*;

enum recrd{
wordInSingleDoc;
};

public class Q2Reducer extends Reducer <Text, Text, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        ArrayList<String> docList = new <String>ArrayList();
        String fileName;
        for (Text val : values) {
            fileName = val.toString();
            int i=0 ;
            for(String doc : docList)
            { 
              if(doc.equals(fileName))
              {
                  i=1;
                  break;
              }
            }
            if(i == 0)
            {  
                docList.add(fileName);
            }
            
        }
        if (docList.size()==1)
        {
            context.getCounter(recrd.wordInSingleDoc).increment(1);
            result.set(docList.size());
            context.write(key, result);
        }
    }

}
