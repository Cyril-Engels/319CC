import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;

public class Main {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("Project4.2");

        JavaSparkContext spark = new JavaSparkContext(sparkConf);

        JavaRDD<String> file = spark.textFile("hdfs:///input");
        JavaRDD<String> words = file.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String line) {
                line = line.trim();
                String[] tokens = line.split("\t");
                String title = tokens[1];
                String text = tokens[3].replaceAll("<[^>]*>", " ")
                                       .replaceAll("\\\\n","\n")
                                       .toLowerCase()
                                       .replaceAll("[^a-z]+", " ")
                                       .trim();
                ArrayList<String> wordTitle = new ArrayList<String>();
                String[] word = text.split(" ");
                for (String s : word) {
                    wordTitle.add(s + "," + title);
                }
                return wordTitle;
            }
        });

        JavaPairRDD<String, Integer> countTotal = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<>(s.split(",", 2)[1], 1);
            }
        });
        final long N = countTotal.distinct().count();

        JavaPairRDD wordInDocuments = words.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) {
                String[] wordTitle = s.split(",", 2);
                return new Tuple2(wordTitle[0], wordTitle[1]);
            }
        });
        final Map map = wordInDocuments.distinct().countByKey();


        JavaPairRDD<String, Integer> wordTitleCount = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2 call(String s) {
                String[] tokens = s.split(",", 2);
                return new Tuple2<>(new Tuple2<>(tokens[0], tokens[1]), 1);
            }
        });
        JavaPairRDD wordTitleCountReduce = wordTitleCount.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer a, Integer b) throws Exception {
                return a + b;
            }
        });

        JavaPairRDD wordCountTitle = wordTitleCountReduce.mapToPair(new PairFunction<Tuple2<Tuple2<String, String>, Integer>, Tuple2<String, String>, Double>() {

            @Override
            public Tuple2<Tuple2<String, String>, Double> call(Tuple2<Tuple2<String, String>, Integer> tuple2) throws Exception {
                Tuple2<String, String> wordTitle = tuple2._1;
                String word = wordTitle._1;
                String title = wordTitle._2;
                long df = (Long) map.get(word);
                int tf = tuple2._2;
                double tfidf = tf * Math.log10(N / df);
                return new Tuple2<>(new Tuple2<>(word, title), tfidf);
            }
        });

        wordCountTitle.saveAsTextFile("hdfs:///output");

    }
}
