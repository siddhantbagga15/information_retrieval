import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BigramCode {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

        Text documentID = new Text();
        Text bigramText = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String fileValue = value.toString();
            String[] documentContent = extractDocumentContent(fileValue);

            String documentIdValue = documentContent[0];
            documentID.set(documentIdValue);
          
            String documentValue = documentContent[1];
            String finalText = cleanText(documentValue);

            StringTokenizer iterator = new StringTokenizer(finalText);
            String previousWord = iterator.nextToken();
            while (iterator.hasMoreTokens()) {
                String currentWord = iterator.nextToken();
                String valueBigram = previousWord + " " + currentWord;
                bigramText.set(valueBigram);
                String wordValue = bigramText.toString();
                if (isTargetBigram(wordValue)) {
                    context.write(bigramText, documentID);
                }
                previousWord = currentWord;
            }
        }
    }

    private static String[] extractDocumentContent(String fileValue) {
        String[] content = fileValue.split("\t", 2);
        return content;
    }

    private static boolean isTargetBigram(String bigram) {
        List<String> targetBigrams = Arrays.asList("computer science", "information retrieval", "power politics", "los angeles", "bruce willis");
        return targetBigrams.contains(bigram);
    }


    private static String cleanText(String inputText) {
        StringBuilder stringBuilder = new StringBuilder();
        for (char c : inputText.toCharArray()) {
            if (Character.isLetter(c)) {
                stringBuilder.append(Character.toLowerCase(c));
            } else {
                stringBuilder.append(' ');
            }
        }
        String preprocessedString = stringBuilder.toString().replaceAll("[^a-z]+", " ");
        return preprocessedString.replaceAll("\\s+", " ");
    }

    public static class IndexReducer extends Reducer<Text, Text, Text, Text> {

        Text finalResult = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            HashMap<String, Integer> hMap = new HashMap<>();

            for (Text value : values) {
                String documentID = value.toString();
                Integer countValue = getCountValue(hMap, documentID);
                hMap.put(documentID, countValue);
            }
        
            String finalString = getOutputString(hMap);

            finalResult.set(finalString);
            context.write(key, finalResult);
        }
    }

    private static Integer getCountValue(HashMap<String, Integer> hMap, String documentID) {
        Integer countValue = hMap.get(documentID);
        if (countValue == null) {
            countValue = 1;
        } else {
            countValue++;
        }
        return countValue;
    }

    private static String getOutputString(HashMap<String, Integer> hMap) {
        StringBuilder stringBuilder = new StringBuilder();
        for (String mapKey : hMap.keySet()) {
            stringBuilder.append(mapKey);
            stringBuilder.append(":");
            Integer countValue = hMap.get(mapKey);
            stringBuilder.append(countValue);
            stringBuilder.append("\t");
        }
        Integer requiredSize = stringBuilder.length() - 1;
        return stringBuilder.substring(0, requiredSize);
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Bigram Index");

        job.setJarByClass(WordCount.class);

        job.setMapperClass(WordCount.TokenizerMapper.class);
        job.setReducerClass(WordCount.IndexReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}