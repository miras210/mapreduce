package org.example;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyKeyComparator extends WritableComparator {
    protected MyKeyComparator() {
        super(CompositeKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CompositeKey k1 = (CompositeKey) a;
        CompositeKey k2 = (CompositeKey) b;
        int cmp = k1.getCount().compareTo(k2.getCount());
        if (cmp != 0) {
            return -1*cmp;
        }
        return k1.getWord().compareTo(k2.getWord());
    }
}
