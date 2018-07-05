

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * KEYIN:��Ӧ����map�׶���������ݵ�key������,Text KEYVALUE:��Ӧ����map�׶���������ݵ�value������,IntWritable
 * KEYOUT:reduceҵ���߼���Ҫ��������ݵ�key������,Text
 * VALUEOUT:reduce�����߼���Ҫ��������ݵ�value����,IntWritable
 * 
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	//һ����ͬkey���������͵���һ������д��reduce����
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		//�ۼ���һ��key�����е�����valueֵ����
		int count = 0;
		for(IntWritable value:values){
			count+=value.get();
		}
		//����ŵ�context�м���
		context.write(key, new IntWritable(count));
	}
}
