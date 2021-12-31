package bigdataTp3;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.StringTokenizer;


public class MyMapper extends Mapper<Object,Text,Text,IntWritable> {
    //private final static IntWritable one = new IntWritable(1);
    //private Text word= new Text();
    public void map(Object key, Text value, Mapper.Context context)
        throws IOException, InterruptedException{

        Scanner input = new Scanner(value.toString());

        while (input.hasNextLine()) {
            String line = input.nextLine();
            List<String> listLine = Arrays.asList(line.split(","));
            context.write(listLine.get(8),listLine.get(7));
        }
        input.close();
      /*  StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()){
            word.set(itr.nextToken());
            context.write(word,one);
        }*/
    }
}
