import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


import java.io.IOException;

import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeSet;

public class model {
    static int counts = 2;
    static int rank = 5;


    public static class ModelMapper extends Mapper<Object, Text, Text, Text> {

        private Text keys = new Text();
        private Text content = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split("\t");
            if ((tokens[0].length() != 0) && (Integer.parseInt(tokens[1]) > counts)) {
                //If the phrase has only one word, directly send it to reducer
                String[] phrase = tokens[0].split(" ");
                if (phrase.length == 1) {
                    keys.set(tokens[0]);
                    content.set(tokens[1]);
                    context.write(keys, content);
                }
                /*If the phrase has more than one words, write to reducer.
                Get the last word and glue the other part together and then send to reducer*/
                if (phrase.length > 1) {
                    keys.set(tokens[0]);
                    content.set(tokens[1]);
                    context.write(keys, content);
                    //Last word as predictor, and the other as key
                    String key2 =phrase[0];
                    String value2 = phrase[phrase.length - 1] + "-" + tokens[1];
                    for (int i = 1; i < phrase.length - 1; i++) {
                        key2 += " " + phrase[i];
                    }
                    keys.set(key2);
                    content.set(value2);
                    context.write(keys, content);
                }

            }
        }

    }


    public static class ModelReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //Use count as a counter for the base of probability
            int count = 0;
            TreeSet<String> tree = new TreeSet<>(new Comparator<String>() {
                @Override
                public int compare(String o1, String o2) {
                    String[] s1 = o1.split("-");
                    String[] s2 = o2.split("-");
                    if (Double.parseDouble(s2[1]) > Double.parseDouble(s1[1])) {
                        return 1;
                    } else {
                        return -1;
                    }
                }
            });
            //The one-word phrase has the count. Add the others to the TreeSet
            for (Text val : values) {
                String[] content = val.toString().split("-");
                if (content.length == 1) {
                    count = Integer.parseInt(content[0]);
                } else {
                    tree.add(val.toString());
                }
            }

            byte[] familyName = Bytes.toBytes("predictor");
            Iterator<String> it = tree.iterator();
            int n = 0;
            //Use iterator to get the first few values in the TreeSet, and put them to HBase table
            while (it.hasNext() && n < rank) {
                n++;
                String Val = it.next().toString();
                byte[] rowKey = Bytes.toBytes(Val.split("-")[0]);
                int num = Integer.parseInt(Val.split("-")[1]);
                double probability = (double) num / count;
                Put p = new Put(Bytes.toBytes(key.toString()));
                p.add(familyName, rowKey, Bytes.toBytes(String.format("%.2f", probability)));
                context.write(new ImmutableBytesWritable(Bytes.toBytes(key.toString())), p);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "ec2-52-4-244-168.compute-1.amazonaws.com");
        Job job = null;
        try {
            job = Job.getInstance(conf, "Ca");
        } catch (IOException e) {
            e.printStackTrace();
        }

        job.setJarByClass(model.class);
        job.setMapperClass(ModelMapper.class);
        job.setReducerClass(ModelReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);


        try {
            TableMapReduceUtil.initTableReducerJob("predicting", ModelReducer.class, job);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            FileInputFormat.addInputPath(job, new Path(args[0]));
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
