import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;

import java.io.IOException;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

public class SpPalindrome {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static boolean isPalindrome(String str){
        return str.equals(new StringBuilder(str).reverse().toString());
    }

    public static void main(String[] args) throws IOException {
        SparkConf sparkConf = new SparkConf()
//                .setMaster("local[*]")
                .setAppName("Example Spark App");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        sparkContext.hadoopConfiguration()
                .setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);
//        JavaRDD<String> lines = sparkContext.textFile("allen-p/*");
        JavaRDD<String> lines = sparkContext.textFile(args[0]);
        String outputPath = args[1]+"/spark/palindrome/"+(new Date()).getTime();
        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s.trim().toLowerCase())).iterator());
        JavaRDD<String> betterWords = words.flatMap(s -> Arrays.asList(s.trim().replaceAll("^[^a-zA-Z0-9\\s]+|[^a-zA-Z0-9\\s]+$", "")).iterator());
        JavaPairRDD<String, Integer> ones = betterWords.mapToPair(s -> new Tuple2<>(s, 1));

//        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) ->(i2>0)? i1 + i2:0);
        JavaPairRDD<String, Integer> counts = ones.filter(s->SpWordCount.isPalindrome(s._1)).sortByKey(true).reduceByKey((i1, i2) -> i1 + i2);
        List<Tuple2<String, Integer>> output = counts.collect();
        counts.saveAsTextFile(outputPath);
        for (Tuple2<?,?> tuple : output) {
            if(tuple._1().toString().length()>0) {
                System.out.println(tuple._1() + ": " + tuple._2());
            }
        }

    }
}

