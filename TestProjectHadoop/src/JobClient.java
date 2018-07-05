

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobClient {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("yarn.resourcemanager.hostname", "localhost");
		
		//创建一个Job提交器对象
		Job job = Job.getInstance(conf);
		
		//告知客户端提交器,我们mr程序所有的jar包
		job.setJarByClass(JobClient.class);
		
		//告知mrappmaster,我们这个程序里面要用的mapper业务实现类和reduce业务实现类
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		//告知mrappmaster,我们这个程序的map阶段和reducer阶段输出的数据类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//告知mrappmaster,我们要求要启动的reduct task的数量是多少
		job.setNumReduceTasks(3);
		
		//告知mrappmaster,我们的程序要处理的数据所有的目录,以及我们要求输出结果所在目录
		FileInputFormat.setInputPaths(job, new Path("d:/1"));
		FileOutputFormat.setOutputPath(job, new Path("d:/2/ceshi"));
		
		//提交job
		Boolean res = job.waitForCompletion(true);
		System.exit(res?0:1);
	}

}
