
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * KEYIN:框架读到的一行数据的起始偏移量,long的值
 * VALUEIN:框架读到的一行数据的内容,String值
 * KEYOUT:我们的业务逻辑要输出的数据的key的类型,在此例中是一个单词,String值
 * VALUEOUT:我们业务逻辑要输出数据的value的类型,在此例中是一个整数,int值
 * 
 * hadoop自己实现了一套虚拟化机制,它的序列化相比jdk的Servizable序列化之后的数据更加精简,从而可以提高网络传输效率
 * 所以在hadoop内部编程中,不要使用java原生数据类型,而要用hadoop中经过包装的数据类型
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	//重定义父Mapper中的map方法,就是我们的业务逻辑方法
	//key其实对应的就是框架要传给我们的参数的KEYIN
	//value其实对应的就是框架要传给我们的参数VALUEIN
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//将拿到的这一行数据按照空格切分
		String line = value.toString();
		String[] lineWords = line.split(" ");
		for(String word:lineWords){
			context.write(new Text(word), new IntWritable(1));
		}

	}
}
