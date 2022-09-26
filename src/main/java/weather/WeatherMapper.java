package weather;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

public class WeatherMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private final Text monthText = new Text();
    private final DoubleWritable average = new DoubleWritable();
    private final Logger logger = Logger.getLogger(WeatherMapper.class);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        logger.info("Init");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() == 0) {
            logger.info("skipped - " + value.toString());
            return;
        }
        String line = value.toString();
        String[] data = line.split(";");
        String date = data[0].substring(4, 11);
        double temp = Float.parseFloat(data[1].substring(1, data[1].length()-1));
        logger.info("key - " + date);
        logger.info("val - " + temp);
        monthText.set(date);
        average.set(temp);
        context.write(monthText, average);
    }
}
