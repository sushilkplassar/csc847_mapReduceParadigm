
import com.google.common.collect.Iterables;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.commons.lang.StringUtils;

import java.util.*;
import java.util.regex.Pattern;

public class SpAnagram {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {

        // STEP-1: handle input parameters
        if (args.length != 3) {
            System.err.println("Usage: [len] [input_path] [output_path] ");
            System.exit(1);
        }

        // if a word.length < N, that word will be ignored
        final int N = Integer.parseInt(args[0]);
        System.out.println("args[0]: Min length = "+N);

        // identify I/O paths
        String inputPath = args[1];
        String outputPath = args[2]+"/spark/anagram/"+(new Date()).getTime();
        System.out.println("args[1]: INPUT PATH is => "+inputPath);
        System.out.println("args[2]: OUTPUT Path is=> "+outputPath);

        // STEP-2: create an instance of JavaSparkContext
        SparkConf sprkConfing = new SparkConf()
//                .setMaster("local[*]")
                .setAppName(" Spark Anagram App");
        JavaSparkContext ctx = new JavaSparkContext(sprkConfing);
        ctx.hadoopConfiguration()
                .setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);
        // STEP-3: create an RDD for input
        // input record format:
        //      word1 word2 word3 ...
        JavaRDD<String> lines = ctx.textFile(inputPath, 1);

        // STEP-4: create (K, V) pairs from input
        // K = sorted(word)
        // V = word
        JavaPairRDD<String, String> resultRDD = lines.flatMapToPair(
                new PairFlatMapFunction<String, String, String>() {
                    @Override
                    public Iterator<Tuple2<String, String>> call(String line) throws Exception {
                        if ((line == null) || (line.length() < N)) {
                            return Collections.EMPTY_LIST.iterator();
                        }

                        String[] words = StringUtils.split(line.trim().toLowerCase());
                        if (words == null) {
                            return Collections.EMPTY_LIST.iterator();
                        }
                        List<Tuple2<String, String>> results = new ArrayList<Tuple2<String, String>>();

                        for (String word : words) {
                            word = word.trim().replaceAll("^[^a-zA-Z0-9\\s]+|[^a-zA-Z0-9\\s]+$", "");
                            if (word.length() < N) {
                                // ignore strings with less than size N
                                continue;
                            }

                            if (word.matches(".*[,.;]$")) {
                                // remove the special char from the end
                                word = word.substring(0, word.length() -1);
                            }
                            if (word.length() < N) {
                                // ignore strings with less than size N
                                continue;
                            }

                            char[] wordChars = word.toCharArray();
                            Arrays.sort(wordChars);
                            String sortedWord = new String(wordChars);
                            results.add(new Tuple2<String, String>(sortedWord, word));
                        }
                        return results.iterator();
                    }

                });

        // STEP-5: create anagrams
        JavaPairRDD<String, Iterable<String>> filteredData = resultRDD.distinct().groupByKey().filter((Function<Tuple2<String, Iterable<String>>, Boolean>) v1 -> {

            if(Iterables.size(v1._2) <= 1)
                return false;
            return true;
        });


        // STEP-6: save output
        filteredData.saveAsTextFile(outputPath);

        // STEP-7: done
        ctx.close();
        System.exit(0);
    }
}
