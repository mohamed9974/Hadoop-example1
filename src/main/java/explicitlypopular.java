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

public class explicitlypopular {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: explicitlypopular <input path> <output path>");
            System.exit(-1);
        }
        Job job = Job.getInstance();
        job.setJarByClass(explicitlypopular.class);
        job.setJobName("explicitlypopular");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(explicitlypopularMapper.class);
        job.setReducerClass(explicitypopularReducer.class);
        job.setPartitionerClass(explicityPartitioner.class);
        job.setNumReduceTasks(2);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class explicitlypopularMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private static final IntWritable totalDuration = new IntWritable(0);
        private final Text explicity = new Text();
        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {
            // Split the line into tokens using the tsv delimiter
            String line = lineText.toString();
            String[] tokens = line.split("\t");
            // Check if the line has the correct number of tokens
            // Check if the song name is not empty
            if (NumberUtils.isParsable(tokens[5])) {
                //set  the explicity
                explicity.set(tokens[3]);
                // Set the duration
                totalDuration.set(NumberUtils.toInt(tokens[5]));
                // Write the key-value pair
                context.write(explicity, totalDuration);
            } else System.out.println("Invalid line: " + line);
        }
    }

    public static class explicitypopularReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(
                Text explicity,
                Iterable<IntWritable> counts,
                Context context
        )
                throws IOException, InterruptedException {
            int sum = 0;
            int average = 0;
            for (IntWritable count : counts) {
                sum += count.get();
                average++;
            }
            context.write(explicity, new IntWritable(sum / average));
        }
    }

    //partitioner
    //part-r-00000 for explicity == TRUE and part-r-00001 for explicity == FALSE
    public static class explicityPartitioner
            extends org.apache.hadoop.mapreduce.Partitioner<Text, IntWritable> {
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            if (key.toString().equals("True")) {
                return 0;
            } else {
                return 1;
            }
        }
    }
}
