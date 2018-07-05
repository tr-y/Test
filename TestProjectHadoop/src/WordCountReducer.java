

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * KEYIN:对应的是map阶段输出的数据的key的类型,Text KEYVALUE:对应的是map阶段输出的数据的value的类型,IntWritable
 * KEYOUT:reduce业务逻辑中要输出的数据的key的类型,Text
 * VALUEOUT:reduce精力逻辑中要输出的数据的value类型,IntWritable
 * 
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	//一组相同key的数据类型调用一次我们写的reduce方法
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		//累计这一组key数据中的所有value值即可
		int count = 0;
		for(IntWritable value:values){
			count+=value.get();
		}
		//输出放到context中即可
		context.write(key, new IntWritable(count));
	}
}
