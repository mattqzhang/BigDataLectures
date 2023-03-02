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

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountTr extends Configured implements Tool{
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

    //public static void main(String[] args) throws Exception {
    public int run(String[] args) throws Exception{
     if (args.length != 2) {
        System.out.printf("Usage: WordCountTr [generic options] <input dir> <output dir>\n");
        System.exit(-1);
      }

      Job job = new Job(getConf());
      job.setJarByClass(WordCountTr.class);
      job.setJobName("Word Count ToolRunner");

      FileInputFormat.setInputPaths(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      job.setMapperClass(WordMapper.class);
      job.setReducerClass(SumReducer.class);

      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(IntWritable.class);

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);

      boolean success = job.waitForCompletion(true);
      return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
      int exitCode = ToolRunner.run(new Configuration(), new WordCountTr(), args);
      System.exit(exitCode);
    }

}
