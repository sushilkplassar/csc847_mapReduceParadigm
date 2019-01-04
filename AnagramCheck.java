import org.apache.hadoop.mapred.MapReduceBase;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import org.apache.hadoop.mapred.Reducer;

import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;


public class AnagramCheck {
    /**
     * The Anagram mapper class gets a word as a line from the HDFS input and
     * sorts the letters in the word and writes its back to the output collector
     * as Key : sorted word (letters in the word sorted) Value: the word itself
     * as the value. When the reducer runs then we can group anagrams together
     * based on the sorted key.
     */

    public static class AnagramMRBaseMapper extends MapReduceBase implements
            Mapper<LongWritable, Text, Text, Text> {
        private Text txtSorted = new Text();
        private Text txtOriginal = new Text();

        @Override
        public void map(LongWritable key, Text value,
                        OutputCollector<Text, Text> outputCollector, Reporter reporter)
                throws IOException {


            String regex = "(.)*(\\d)(.)*";
            Pattern pattern = Pattern.compile(regex);

            StringTokenizer line = new StringTokenizer(value.toString().toLowerCase());
            while (line.hasMoreTokens()) {
                String word =
                        line.nextToken().replaceAll("^[^a-zA-Z0-9\\s]+|[^a-zA-Z0-9\\s]+$", "");

                if (word.length() <= 1 || pattern.matcher(word).matches())
                    continue;

                char[] wordChars = word.toCharArray();
                Arrays.sort(wordChars);
                String sortedAlphabetsKey = new String(wordChars);
                txtSorted.set(sortedAlphabetsKey);
                txtOriginal.set(word);
                outputCollector.collect(txtSorted, txtOriginal);
            }
        }
    }
    /**
     * The Anagram reducer class groups the values of the sorted keys that came
     * in and checks to see if the values iterator contains more than one word.
     * if the values contain more than one word we have spotted a anagram.
     */

    public static class AnagramMRBaseReducer extends MapReduceBase implements
            Reducer<Text, Text, Text, Text> {

        private Text opKey = new Text();
        private Text opVal = new Text();

        @Override
        public void reduce(Text anaSortedKey, Iterator<Text> anaWordVal,
                           OutputCollector<Text, Text> results, Reporter reporter)
                throws IOException {
            String strOutput = "";

            Set<String> uniqueAnagrams = new HashSet<>();

            while (anaWordVal.hasNext()) {
                uniqueAnagrams.add(anaWordVal.next().toString());
//                Text wrdAnagram = anaWordVal.next();
//                strOutput = strOutput + wrdAnagram.toString() + "~";
            }
            strOutput = StringUtils.join("`",uniqueAnagrams);

            StringTokenizer outputTokenizer = new StringTokenizer(strOutput, "`");
            if (outputTokenizer.countTokens() >= 2) {
                strOutput = strOutput.replace("`", ",");
                opKey.set(anaSortedKey.toString());
                opVal.set(strOutput);
                results.collect(opKey, opVal);
            }

        }

    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(AnagramCheck.class);
        conf.setJobName("Anagram Checker");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(AnagramMRBaseMapper.class);
        conf.setReducerClass(AnagramMRBaseReducer.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        Job job = Job.getInstance(conf);
        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

//        JobClient.runJob(job);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
