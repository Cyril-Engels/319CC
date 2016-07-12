import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 *
 */
public class ngram {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().toLowerCase().replaceAll("[^a-z]", " ").trim().replaceAll(" {2,}", " ");
            if (line.length() != 0) {
                String[] tokens = line.split(" ");
                int length = tokens.length;
                for (int i = 0; i < length; i++) {
                    word.set(tokens[i]);
                    context.write(word, one);
                }
                for (int i = 0; i < length - 1; i++) {
                    word.set(tokens[i] + " " + tokens[i + 1]);
                    context.write(word, one);
                }
                for (int i = 0; i < length - 2; i++) {
                    word.set(tokens[i] + " " + tokens[i + 1] + " " + tokens[i + 2]);
                    context.write(word, one);
                }
                for (int i = 0; i < length - 3; i++) {
                    word.set(tokens[i] + " " + tokens[i + 1] + " " + tokens[i + 2] + " " + tokens[i + 3]);
                    context.write(word, one);
                }
                for (int i = 0; i < length - 4; i++) {
                    word.set(tokens[i] + " " + tokens[i + 1] + " " + tokens[i + 2] + " " + tokens[i + 3] + " " + tokens[i + 4]);
                    context.write(word, one);
                }
            }
        }
    }


    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(ngram.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}