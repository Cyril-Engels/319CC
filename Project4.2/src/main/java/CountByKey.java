import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;

/**
 * This class calculates the TF-IDF value of word 'cloud' in documents.
 *
 * @author Yilong Chang
 * @version 2.0
 * @since 04/25/2015
 */
public class CountByKey {
    public static void main(String[] args) {
        String url = "s3n://s15-p42-part1-easy/data/";
        // write your code here
        SparkConf sparkConf = new SparkConf().setAppName("Project4.2");
        JavaSparkContext spark = new JavaSparkContext(sparkConf);

        JavaRDD<String> file = spark.textFile(url);
        //JavaRDD<String> file = spark.textFile("hdfs:///input");
        JavaRDD<String> words = file.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String line) {
                String[] tokens = line.split("\t");
                String title = tokens[1];
                String text = tokens[4];
                text = text.replaceAll("\\\\n", "\n");
                text = text.toLowerCase().replaceAll("[^a-z]", " ").trim().replaceAll(" +", " ");
                ArrayList<String> wordTitle = new ArrayList<String>();
                String[] word = text.split(" ");
                for (String s : word) {
                    wordTitle.add(s + "," + title);
                }
                return wordTitle;
            }
        });
        //Compute total number of documents
        JavaPairRDD<String, Integer> countTotal = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s.split(",", 2)[1], 1);
            }
        });
        final long N = countTotal.distinct().count();//total number of documents

        //Compute the number of times a word occurs in one document
        JavaPairRDD<String, Integer> wordTitleCount = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2 call(String s) {
                String[] tokens = s.split(",", 2);
                return new Tuple2<Tuple2<String, String>, Integer>(new Tuple2<String, String>(tokens[0], tokens[1]), 1);
            }
        });
        JavaPairRDD wordTitleCountReduce = wordTitleCount.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer a, Integer b) throws Exception {
                return a + b;
            }
        });

        //Compute the number of documents a word occurs in
        JavaPairRDD wordTitle = wordTitleCountReduce.mapToPair(new PairFunction<Tuple2<Tuple2<String, String>, Integer>, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(Tuple2<Tuple2<String, String>, Integer> tuple2) throws Exception {
                Tuple2<String, String> wordTitle = tuple2._1;
                String word = wordTitle._1;
                return new Tuple2<String, Integer>(word, 1);
            }
        });
        JavaPairRDD wordDF = wordTitle.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer a, Integer b) throws Exception {
                return a + b;
            }
        });
        //Change the ((word, title), count) to (word, count+","+title)
        JavaPairRDD wordTFTitle = wordTitleCountReduce.mapToPair(new PairFunction<Tuple2<Tuple2<String, String>, Integer>, String, String>() {

            @Override
            public Tuple2<String, String> call(Tuple2<Tuple2<String, String>, Integer> tuple2) throws Exception {
                Tuple2<String, String> wordTitle = tuple2._1;

                return new Tuple2<String, String>(wordTitle._1, tuple2._2 + "," + wordTitle._2);
            }
        });
        JavaPairRDD wordTFTitleDF = wordTFTitle.join(wordDF);
        //Calculate TF-IDF value for word 'cloud', and return "0\tcloud" for documents that don't contain 'cloud'
        JavaRDD wordTitleTFIDF = wordTFTitleDF.map(new Function<Tuple2<String, Tuple2<String, Integer>>, String>() {
            @Override
            public String call(Tuple2<String, Tuple2<String, Integer>> tuple2) throws Exception {
                String word = tuple2._1;
                if (word.equals("cloud")) {
                    Tuple2<String, Integer> tFTitleDF = tuple2._2;
                    String tfTitle = tFTitleDF._1;
                    String[] tokens = tfTitle.split(",", 2);
                    int tf = Integer.parseInt(tokens[0]);
                    String title = tokens[1];
                    double df = tFTitleDF._2;
                    double tfidf = tf * Math.log10(N / df);
                    return title + "\t" + tfidf;
                }
                return "0\tcloud";
            }
        });
        //Only output those documents that contain 'cloud'
        JavaRDD filter = wordTitleTFIDF.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return !s.split("\t")[1].equals("cloud");
            }
        });
        filter.saveAsTextFile("hdfs:///output");

    }
}
