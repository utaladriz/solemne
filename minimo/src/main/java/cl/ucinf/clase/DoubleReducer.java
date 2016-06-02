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
      Double min = Double.MAX_VALUE;
      for (DoubleWritable val : values) {
        if (min > val.get())
           min = val.get();
      }
      result.set(min);
      context.write(key, result);
    }
  }