package cl.ucinf.clase;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created by utaladriz on 16-05-16.
 */

public class DoubleReducer 
       extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();

    public void reduce(Text key, Iterable<DoubleWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      Double max = Double.MIN_VALUE;
      for (DoubleWritable val : values) {
        if (max < val.get())
           max = val.get();
      }
      result.set(max);
      context.write(key, result);
    }
  }