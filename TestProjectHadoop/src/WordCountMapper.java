
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * KEYIN:��ܶ�����һ�����ݵ���ʼƫ����,long��ֵ
 * VALUEIN:��ܶ�����һ�����ݵ�����,Stringֵ
 * KEYOUT:���ǵ�ҵ���߼�Ҫ��������ݵ�key������,�ڴ�������һ������,Stringֵ
 * VALUEOUT:����ҵ���߼�Ҫ������ݵ�value������,�ڴ�������һ������,intֵ
 * 
 * hadoop�Լ�ʵ����һ�����⻯����,�������л����jdk��Servizable���л�֮������ݸ��Ӿ���,�Ӷ�����������紫��Ч��
 * ������hadoop�ڲ������,��Ҫʹ��javaԭ����������,��Ҫ��hadoop�о�����װ����������
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	//�ض��常Mapper�е�map����,�������ǵ�ҵ���߼�����
	//key��ʵ��Ӧ�ľ��ǿ��Ҫ�������ǵĲ�����KEYIN
	//value��ʵ��Ӧ�ľ��ǿ��Ҫ�������ǵĲ���VALUEIN
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//���õ�����һ�����ݰ��տո��з�
		String line = value.toString();
		String[] lineWords = line.split(" ");
		for(String word:lineWords){
			context.write(new Text(word), new IntWritable(1));
		}

	}
}
