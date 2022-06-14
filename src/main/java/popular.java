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

public class popular {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: popular <input path> <output path>");
            System.exit(-1);
        }
        // create a Hadoop job and set the main class
        Job job = Job.getInstance();
        job.setJarByClass(popular.class);
        job.setJobName("popular");

        // set the input and output path
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // set the Mapper and Reducer class
        job.setMapperClass(popularMapper.class);
        job.setReducerClass(popularReducer.class);

        // specify the type of the output
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // run the job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class popularMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        // the value of the key is the popularity integer
        // the key is the artist name
        private final Text artistName = new Text();
        private final IntWritable popularity = new IntWritable(0);

        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {
            // Split the line into tokens using the tsv delimiter
            String line = lineText.toString();
            String[] tokens = line.split("\t");
            // Check if the line has the correct number of tokens
            // Check if the artist name is not empty
            if (NumberUtils.isParsable(tokens[5])) {
                // Set the artist name
                artistName.set(tokens[0]);
                // Set the popularity
                popularity.set(NumberUtils.toInt(tokens[5]));
                // Write the key-value pair
                context.write(artistName, popularity);
            } else System.out.println("Invalid line: " + line);
        }
    }

    public static class popularReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(
                Text artistName,
                Iterable<IntWritable> counts,
                Context context
        )
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable count : counts) {
                sum += count.get();
            }
            context.write(artistName, new IntWritable(sum));
        }
    }
}
