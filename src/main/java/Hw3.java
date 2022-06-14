//this file would take the input from the user and will decide which class to use
// the input command line is:
// hadoop jar Hw3.jar Hw3 <class> <input path> <output path>
// the input path is the path to the input file
// the output path is the path to the output file
// the class is the class to use

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Hw3 {

  public static List<String[]> parse(Path csvFiledirectory) throws IOException {
    List<String[]> records = new ArrayList<String[]>();
    FileReader fileReader = new FileReader(csvFiledirectory.toString());
    //remove the header
    CSVParser csvParser = new CSVParser(fileReader, CSVFormat.DEFAULT.withHeader());
    for (CSVRecord csvRecord : csvParser) {
      String[] record = new String[csvRecord.size()];
      for (int i = 0; i < csvRecord.size(); i++) {
        record[i] = csvRecord.get(i);
      }
      records.add(record);
    }
    csvParser.close();
    return records;
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      System.err.println("Usage: <class> <input path> <output path>");
      System.exit(-1);
    }
    String className = args[0];
    // Set the class name
    Class<?> clazz = Class.forName(className);
    Class<? extends Mapper> mapperClass = null;
    Class<? extends Reducer> reducerClass = null;
    Class<? extends Partitioner> partitionerClass = null;
    
    // Set the mapper and reducer class
    if (clazz == total.class) {
      mapperClass = total.totalMapper.class;
      reducerClass = total.totalReducer.class;
    } else if (clazz == average.class) {
      mapperClass = average.DurationMapper.class;
      reducerClass = average.DurationReducer.class;
    } else if (clazz == popular.class) {
      mapperClass = popular.popularMapper.class;
      reducerClass = popular.popularReducer.class;
    } else if (clazz == explicitlypopular.class) {
      mapperClass = explicitlypopular.explicitlypopularMapper.class;
      reducerClass = explicitlypopular.explicitypopularReducer.class;
      partitionerClass = explicitlypopular.explicityPartitioner.class;
    } else if (clazz == dancebyyear.class) {
      mapperClass = dancebyyear.dancebyyearMapper.class;
      reducerClass = dancebyyear.dancebyyearReducer.class;
      partitionerClass = dancebyyear.dancebyyearPartitioner.class;
    } else {
      System.err.println("Invalid class name");
      System.exit(-1);
    }
    // get clazz subclass of Mapper or Reducer and assign it to mapper or reducer
    Job job = new Job();
    // get the mapper and reducer class and send them to the job without incurring
    // the strinrg conversion error
    // if the className is explicitlypopular or dancebyyear, then use the
    // partitioner
    if (className.equals("explicitlypopular") || className.equals("dancebyyear")) {
      job.setPartitionerClass(partitionerClass);
    }
    // set the mapper and reducer class
    // get the mapper subclass from clazz
    job.setMapperClass(mapperClass);
    // get the reducer subclass from clazz
    job.setReducerClass(reducerClass);

    // Set the input path
    // parse the input file using the csvparser class main method and get the newly
    // genrated file's path as the input path

    Path inputPath = new Path(args[1]);
    // parse the input file and get the path of the newly generated file
    FileWriter writer = new FileWriter("output.tsv");
    // create a file writer to write the output to a file
    // no heading is needed for the output file
    CSVPrinter printer = new CSVPrinter(writer, CSVFormat.TDF);
    // create a csv printer to write the output to a file
    List<String[]> list = parse(inputPath);
    // parse the input file and get the list of the lines
    for (String[] line : list) {
      printer.printRecord(line);
      // write the lines to the file
    }
    // close the printer
    printer.close();
    // close the writer
    writer.close();
    // get the path of the newly generated file
    Path newInputPath = new Path("output.tsv");
    // set the input path to the newly generated file
    FileInputFormat.addInputPath(job, newInputPath);
    // get the output path
    Path outputPath = new Path(args[2]);
    //check if the output path is already there if true remove it first
    if (outputPath.getFileSystem(job.getConfiguration()).exists(outputPath)) {
      outputPath.getFileSystem(job.getConfiguration()).delete(outputPath, true);
    }


    // Create a job
    // Set the job name
    job.setJobName(className);
    // Set the jar file
    job.setJarByClass(clazz);
    // Set the mapper class
    job.setMapperClass((Class<? extends Mapper>) mapperClass);
    // Set the reducer class
    job.setReducerClass((Class<? extends Reducer>) reducerClass);
    // Set the output key class
    job.setOutputKeyClass(Text.class);
    // Set the output value class float writable if the class is explicitypopular or
    // dancebyyear
    if (className.equals("dancebyyear")) {
      job.setOutputValueClass(FloatWritable.class);
    } else {
      job.setOutputValueClass(IntWritable.class);
    }
    // set up a partitioner if the class is explicitypopular or dancebyyear
    if (className.equals("explicitlypopular")) {
      job.setNumReduceTasks(2);
    }
    if (className.equals("dancebyyear")) {
      job.setNumReduceTasks(3);
    }

    // Set the input path
    // Set the output path
    FileOutputFormat.setOutputPath(job, outputPath);
    // Run the job
    job.waitForCompletion(true);
  }
}
