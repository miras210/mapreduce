package org.example;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortingReducer extends Reducer<CompositeKey, CompositeKey, NullWritable, CompositeKey> {
    private final NullWritable nullWritable = NullWritable.get();

    @Override
    protected void reduce(CompositeKey key, Iterable<CompositeKey> values, Context context) throws IOException, InterruptedException {
        for(CompositeKey k: values) {
            context.write(nullWritable, k);
        }
    }
}
