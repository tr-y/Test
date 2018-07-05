

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
		
		//����һ��Job�ύ������
		Job job = Job.getInstance(conf);
		
		//��֪�ͻ����ύ��,����mr�������е�jar��
		job.setJarByClass(JobClient.class);
		
		//��֪mrappmaster,���������������Ҫ�õ�mapperҵ��ʵ�����reduceҵ��ʵ����
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		//��֪mrappmaster,������������map�׶κ�reducer�׶��������������
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//��֪mrappmaster,����Ҫ��Ҫ������reduct task�������Ƕ���
		job.setNumReduceTasks(3);
		
		//��֪mrappmaster,���ǵĳ���Ҫ������������е�Ŀ¼,�Լ�����Ҫ������������Ŀ¼
		FileInputFormat.setInputPaths(job, new Path("d:/1"));
		FileOutputFormat.setOutputPath(job, new Path("d:/2/ceshi"));
		
		//�ύjob
		Boolean res = job.waitForCompletion(true);
		System.exit(res?0:1);
	}

}
