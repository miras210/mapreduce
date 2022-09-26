package weather;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WeatherReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private final DoubleWritable avg = new DoubleWritable();

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double sum = 0;
        int count = 0;
        for (DoubleWritable val: values) {
            sum += val.get();
            count++;
        }
        avg.set(sum/count);
        context.write(key, avg);
    }
}
