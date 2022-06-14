import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class total {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: total <input path> <output path>");
            System.exit(-1);
        }
        // create a Hadoop job and set the main class
        Job job = Job.getInstance();
        job.setJarByClass(total.class);
        job.setJobName("total");

        // set the input and output path
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // set the Mapper and Reducer class
        job.setMapperClass(totalMapper.class);
        job.setReducerClass(totalReducer.class);

        // specify the type of the output
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // run the job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class totalMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private static final IntWritable totalDuration = new IntWritable(0);
        private final Text songName = new Text("total");
        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {
            // Split the line into tokens using the tsv delimiter
            String line = lineText.toString();
            String[] tokens = line.split("\t");
            // Check if the line has the correct number of tokens
            // Check if the song name is not empty
            if (NumberUtils.isParsable(tokens[2])) {
                // Set the duration
                totalDuration.set(NumberUtils.toInt(tokens[2]));
                // Write the key-value pair
                context.write(songName, totalDuration);
            } else System.out.println("Invalid line: " + line);
        }
    }

    public static class totalReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(
                Text songName,
                Iterable<IntWritable> counts,
                Context context
        )
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable count : counts) {
                sum += count.get();
            }
            context.write(songName, new IntWritable(sum));
        }
    }
}
