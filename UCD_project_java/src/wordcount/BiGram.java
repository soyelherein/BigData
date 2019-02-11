/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package wordcount;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import static java.lang.System.in;
import static java.lang.System.out;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 *
 * @author user1
 */
public class BiGram implements Writable,WritableComparable<BiGram>{
    
    private Text word = new Text();
    private IntWritable count = new IntWritable();
    
    BiGram()
    {
    }
    
    BiGram(String word,int count)
    {
        this.word.set(word);
        this.count.set(count);
    }
    
    public void setWd(Text word)
    {   this.word.set(word);
    
    }
    
    public void setCnt(int cnt)
    {   this.count.set(cnt);
    
    }
    
    public Text getWd()
    {   return this.word;
    
    }
    
    public IntWritable getCnt()
    {   return this.count;
    
    }
    
    public String toString()
    {
        return "{"+getWd()+","+getCnt()+"}";
    }
    
    public int compareTo(BiGram bg)
    {
        int val = this.word.compareTo(bg.word);
        
        if (val == 0)
        {
            val = this.count.compareTo(bg.count);
        }
      return val;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        word.write(out);
	count.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    	word.readFields(in);
	count.readFields(in);
    
    }
    
        @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BiGram that = (BiGram) o;

        if (count != null ? !count.equals(that.count) : that.count != null) return false;
        if (word != null ? !word.equals(that.word) : that.word != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = word != null ? word.hashCode() : 0;
        result = 10 * result + (count != null ? count.hashCode() : 0);
        return result;
}
    
}
