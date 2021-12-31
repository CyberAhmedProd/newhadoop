package bigdataTp3;
// import libraries
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
public class AverageAge {
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        // private text gender variable which
        // stores the gender of the person
        // who died in the Titanic Disaster
        private Text gender = new Text();

        // private IntWritable variable age will store
        // the age of the person for MapReduce. where
        // key is gender and value is age
        private IntWritable age = new IntWritable();

        // overriding map method(run for one time for each record in dataset)
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {

            // storing the complete record
            // in a variable name line
            String line = value.toString();

            // splitting the line with ', ' as the
            // values are separated with this
            // delimiter
            String str[] = line.split(",");

            /* checking for the condition where the
               number of columns in our dataset
               has to be more than 6. This helps in
               eliminating the ArrayIndexOutOfBoundsException
               when the data sometimes is incorrect
               in our dataset*/
            if (str.length > 9) {

                // storing the gender
                // which is in 8th column
                gender.set(str[8]);
                // checking for numeric data with
                // the regular expression in this column
                if (str[7].matches("\\d+")) {
                    // storing the person's age in column 7
                    age.set(Integer.parseInt(str[7]));
                }

            }
            // writing key and value to the context
            // which will be output of our map phase
            context.write(gender, age);
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        // overriding reduce method(runs each time for every key )
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException
        {

            // declaring the variable sum which
            // will store the sum of ages of people
            int sum = 0;

            // Variable l keeps incrementing for
            // all the value of that key.
            int l = 0;

            // foreach loop
            for (IntWritable val : values) {
                l += 1;
                // storing and calculating
                // sum of values
                sum += val.get();
            }
            sum = sum / l;
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();

        @SuppressWarnings("deprecation")
        Job job = new Job(conf, "Averageage_diabetes");
        job.setJarByClass(AverageAge.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        Path out = new Path(args[1]);
        out.getFileSystem(conf).delete(out);
        job.waitForCompletion(true);
    }
}
