package org.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class SortingMapper extends Mapper<Object, Text, CompositeKey, CompositeKey> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString(), " \n");
        while (itr.hasMoreTokens()) {
            String pair = itr.nextToken();
            CompositeKey k = new CompositeKey();
            String[] split = pair.trim().split("\t");
            k.setWord(split[0]);
            k.setCount(Integer.parseInt(split[1]));

            context.write(k, k);
        }
    }
}
