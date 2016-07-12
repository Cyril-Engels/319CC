import com.google.common.base.Optional;
import org.apache.spark.Accumulator;
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
import java.util.Collection;
import java.util.Iterator;

/**
 * This class compute the PageRank of pages iteratively.
 *
 * @author Yilong Chang
 * @since 04/26/2015
 * @version 1.0
 */
public class PageRank {
    public static void main(String[] args) {
        //String url = "s3n://s15-p42-part2/";

        SparkConf sparkConf = new SparkConf().setAppName("Project4.2");
        JavaSparkContext spark = new JavaSparkContext(sparkConf);
        sparkConf.set("spark.executor.memory", "26g");
        //JavaRDD<String> file = spark.textFile(url);
        JavaRDD<String> file = spark.textFile("hdfs:///input");
        //Form an adjacency list
        JavaPairRDD<String, Iterable<String>> pageNeighbours = file.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                String[] tokens = s.split("\t");
                String page = tokens[0];
                String neighbour = tokens[1];
                return new Tuple2<String, String>(page, neighbour);
            }
        }).distinct().groupByKey().cache();

        //Calculate total page number by adding two columns to one ArrayList, and count the number
        JavaRDD<String> pages = file.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                ArrayList<String> pages = new ArrayList<String>();
                String[] pair = s.trim().split("\t");
                pages.add(pair[0]);
                pages.add(pair[1]);
                return pages;
            }
        }).distinct().cache();
        final long N = pages.count();

        //Give each page an initial PageRank 1.0
        JavaPairRDD<String, Double> pageScore = pages.mapToPair(new PairFunction<String, String, Double>() {
            @Override
            public Tuple2<String, Double> call(String s) throws Exception {
                return new Tuple2<String, Double>(s, 1.0);
            }
        });
//        //Calculate the size of each Iterable
//        JavaPairRDD<String, Integer> neighbourSize = pageNeighbours.mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, String, Integer>() {
//            @Override
//            public Tuple2<String, Integer> call(Tuple2<String, Iterable<String>> tuple2) throws Exception {
//                String page = tuple2._1;
//                Iterable<String> iterable = tuple2._2;
//                int linkSize = ((Collection<?>) iterable).size();
//                return new Tuple2<String, Integer>(page, linkSize);
//            }
//        });
        //Compute PageRank iteratively
        for (int i = 0; i < 10; i++) {
            // Use an accumulator to accumulate scores of dangling pages
            final Accumulator<Double> danglingPageScore = spark.accumulator(0.0);
            //Use outer join to distinguish normal pages and dangling pages
            JavaPairRDD<String, Tuple2<Double,Optional<Iterable<String>>>> pageScoreNeighbours = pageScore.leftOuterJoin(pageNeighbours);
            //Compute the score to distribute for normal pages and add up all scores for dangling pages
            JavaRDD<Tuple2<String, Double>> pageContrib = pageScoreNeighbours.flatMap(new FlatMapFunction<Tuple2<String, Tuple2<Double, Optional<Iterable<String>>>>, Tuple2<String, Double>>() {
                @Override
                public Iterable<Tuple2<String, Double>> call(Tuple2<String, Tuple2<Double, Optional<Iterable<String>>>> tuple2) throws Exception {
                    double currentScore = tuple2._2._1;
                    Tuple2<Double, Optional<Iterable<String>>> scoreNeighbours = tuple2._2;
                    ArrayList<Tuple2<String, Double>> pageContribute = new ArrayList<Tuple2<String, Double>>();
                    //Check if current page has neighbours and distribute score if yes
                    if (scoreNeighbours._2.isPresent()) {
                        int neighboursCount = ((Collection<String>) scoreNeighbours._2.get()).size();
                        Iterator<String> iterator = ((Collection<String>) scoreNeighbours._2.get()).iterator();
                        while (iterator.hasNext()) {
                            pageContribute.add(new Tuple2<String, Double>(iterator.next(), currentScore / neighboursCount));
                        }
                    } else {//Add
                        danglingPageScore.add(currentScore);
                    }
                    return pageContribute;
                }
            });
            //Use an action to force above transformation to execute
            pageContrib.count();
            //Sum incoming links' contributions.
            JavaPairRDD<String, Double> acquiredNormalScore = JavaPairRDD.fromJavaRDD(pageContrib).reduceByKey(new Function2<Double, Double, Double>() {
                @Override
                public Double call(Double a, Double b) throws Exception {
                    return a + b;
                }
            });
            final double totalDanglingScore = danglingPageScore.value();//Get total dangling scores

            //Comoute new score by adding total incoming links' contributions and dangling score
            pageScore = acquiredNormalScore.mapToPair(new PairFunction<Tuple2<String, Double>, String, Double>() {
                @Override
                public Tuple2<String, Double> call(Tuple2<String, Double> tuple2) throws Exception {
                    double add = tuple2._2;
                    double newScore = 0.15 + 0.85 * (add + totalDanglingScore / N);
                    return new Tuple2<String, Double>(tuple2._1, newScore);
                }
            });
        }
        JavaRDD<String> mappingFile = spark.textFile("s3n://s15-p42-part2/wikipedia_mapping");
        //Map the page id to its title
        JavaPairRDD<String,String> pageTitle = mappingFile.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String line) throws Exception {
                String[] tokens = line.split("\t");
                String page = tokens[0];
                String title = tokens[1];
                return new Tuple2<String, String>(page, title);
            }
        });

        JavaRDD<String> titlePageRank = pageTitle.join(pageScore).map(new Function<Tuple2<String, Tuple2<String, Double>>, String>() {
            @Override
            public String call(Tuple2<String, Tuple2<String, Double>> tuple2) throws Exception {
                String title = tuple2._2._1;
                Double pageRank = tuple2._2._2;
                return title + "\t" + pageRank;
            }
        });
        titlePageRank.saveAsTextFile("hdfs:///output2/");
    }
}