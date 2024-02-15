import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.slf4j.LoggerFactory;

import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SleepTweetsTime {

    public static class CustomRecordReader extends RecordReader<LongWritable, Text> {
        private LineRecordReader lineReader;
        private int linesToRead = 4;
        private LongWritable key;
        private Text value;

        public CustomRecordReader() {
            lineReader = new LineRecordReader();
        }

        @Override
        public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
            lineReader.initialize(genericSplit, context);
        }

        @Override
        public boolean nextKeyValue() throws IOException {
            if (key == null) {
                key = new LongWritable();
            }
            if (value == null) {
                value = new Text();
            }
            
            List<String> lines = new ArrayList<>();
            while (lines.size() < linesToRead && lineReader.nextKeyValue()) {
                key.set(lineReader.getCurrentKey().get());
                lines.add(lineReader.getCurrentValue().toString());
            }
            
            if (lines.isEmpty()) {
                return false;
            }

            StringBuilder mergedLines = new StringBuilder();
            for (String line : lines) {
                mergedLines.append(line).append("\n");
            }

            value.set(mergedLines.toString());
            return true;
        }

        @Override
        public LongWritable getCurrentKey() {
            return key;
        }

        @Override
        public Text getCurrentValue() {
            return value;
        }

        @Override
        public float getProgress() throws IOException {
            return lineReader.getProgress();
        }

        @Override
        public void close() throws IOException {
            lineReader.close();
        }
    }

    public static class CustomInputFormat extends FileInputFormat<LongWritable, Text> {

        @Override
        public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
            return new CustomRecordReader();
        }
    
        @Override
        protected boolean isSplitable(JobContext context, Path filename) {
            return false;
        }
    }
    public static class TweetHourMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text hour = new Text();

        //private static final org.slf4j.Logger Log = LoggerFactory.getLogger(TweetHourMapper.class);
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] lines = value.toString().split("\n");
            //for(String line : lines) Log.info("line: "+line);
            //lines[0] emptyline
            //lines[1] timestamp
            //lines[3] is post
            if (lines.length >= 4 && lines[3].toString().toLowerCase().contains("sleep")) {
                //time is in lines[0]
                String[] fields = lines[1].split(" |\t");
                if (fields.length >= 3 && fields[0].equals("T")) {
                    String tweetHour = fields[2].split(":")[0];
                    hour.set(tweetHour);
                    context.write(hour, one);
                }
            }
        }
    }

    public static class TweetHourReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
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
        Job job = Job.getInstance(conf, "SleepTweetsTime");
        job.setJarByClass(SleepTweetsTime.class);
        job.setInputFormatClass(CustomInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));

        job.setMapperClass(TweetHourMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(TweetHourReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}