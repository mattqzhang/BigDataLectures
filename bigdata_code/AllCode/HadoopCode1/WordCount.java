import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Reducer;


public class WordCount {
    public static class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable>


    public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable>


    public static void main(String[] args) throws Exception {


}