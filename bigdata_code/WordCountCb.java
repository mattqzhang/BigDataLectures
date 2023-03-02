import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountCb {
    public static class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable>
    {
      @Override
      public void map(LongWritable key, Text value, Context context)
         throws IOException, InterruptedException {

        String line = value.toString();
        for (String word : line.split("\\W+")) {
          if (word.length() > 0) {
            context.write(new Text(word), new IntWritable(1));
           }
        }
      }
    }

    public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable>
    {
      @Override
      public void reduce(Text key, Iterable<IntWritable> values, Context context)
         throws IOException, InterruptedException {

        int wordCount = 0;
        for (IntWritable value : values) {
           wordCount += value.get();
        }
        context.write(key, new IntWritable(wordCount));
      }
    }

    public static void main(String[] args) throws Exception {
      if (args.length != 2) {
        System.out.printf("Usage: WordCountCb <input dir> <output dir>\n");
        System.exit(-1);
      }

      Job job = new Job();
      job.setJarByClass(WordCountCb.class);
      job.setJobName("Word Count Combiner");

      FileInputFormat.setInputPaths(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      job.setMapperClass(WordMapper.class);
      job.setReducerClass(SumReducer.class);
      job.setCombinerClass(SumReducer.class);

      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(IntWritable.class);

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);

      boolean success = job.waitForCompletion(true);
      System.exit(success ? 0 : 1);
    }
}
