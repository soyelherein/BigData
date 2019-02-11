package wordcount;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class Q3NaturalKeyGroupingComparator extends WritableComparator {

	protected Q3NaturalKeyGroupingComparator() {
		super(BiGram.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		BiGram k1 = (BiGram)w1;
		BiGram k2 = (BiGram)w2;
		
		return k1.getWd().compareTo(k2.getWd());
	}
}
