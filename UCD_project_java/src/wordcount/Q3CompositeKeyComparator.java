package wordcount;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Q3CompositeKeyComparator extends WritableComparator{
    
    private Q3CompositeKeyComparator()
    {
        super(BiGram.class,true);
    }
    
    @SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		BiGram k1 = (BiGram)w1;
		BiGram k2 = (BiGram)w2;
		int result = (-1)*k1.getCnt().compareTo(k2.getCnt());
		if(0 == result) {
			result = (k1.compareTo(k2));
		}
		return result;
	}
    
    
}
