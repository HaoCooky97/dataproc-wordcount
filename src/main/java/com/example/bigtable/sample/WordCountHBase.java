package com.example.bigtable.sample;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCountHBase {
  public static void main(String [] args) throws Exception
  {
    Configuration c=new Configuration();
    String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
    Path input=new Path(files[0]);
    Path output=new Path(files[1]);
    Job j=new Job(c,"wordcount");
    j.setJarByClass(WordCountHBase.class);
    j.setMapperClass(MapForWordCount.class);
    j.setReducerClass(ReduceForWordCount.class);
    j.setOutputKeyClass(Text.class);
    j.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(j, input);
    FileOutputFormat.setOutputPath(j, output);
    System.exit(j.waitForCompletion(true)?0:1);
  }
  //Class MAP
  public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable>
  {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
    	// Delete output if exists
    FileSystem hdfs = FileSystem.get(conf);
    if (hdfs.exists(outputDir))
      hdfs.delete(outputDir, true);

    // Execute job
    int code = job.waitForCompletion(true) ? 0 : 1;
    System.exit(code);
	      //lấy thông tin trong file input chuyển sang 1 chuổi kiểu string
	String line = value.toString();
	//tách từng số bỏ vào mảng string cứ đụng 1 dấu phẩy là lấy được 1 số
	String[] words=line.split(",");
	for(String word: words )
	{
		//khởi tạo key out put
		Text outputKey = new Text(word.toUpperCase().trim());
		//khởi tạo biến lưu giá trị của key out put
		IntWritable outputValue = new IntWritable(1);
		//kiểm tra xem chiều dài từ có trên 4 không
		//nếu đúng thì ghi vào cập giá trị <key,value>
		if(word.length()>=4){
		con.write(outputKey, outputValue);
			}	
	}
  }
  //Class REDUCE
  public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable>
  {
    public void reduce(Text word, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    {
      int sum = 0;
        for(IntWritable value : values)
        {
        sum += value.get();
        }
        context.write(word, new IntWritable(sum));
    }
  }
}


