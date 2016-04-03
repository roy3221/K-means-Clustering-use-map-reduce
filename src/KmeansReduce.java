import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;


public class KmeansReduce extends TableReducer<IntWritable, IntWritable, ImmutableBytesWritable>  {
	
	
	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws 
	IOException, InterruptedException {
		System.out.println("This is reducer");
	int sum=0;
	long newCenter=0;
	List<Integer> cluster= new ArrayList<Integer>();
	for (IntWritable val : values) {
		sum+=val.get();
		cluster.add(val.get());
	}
	
	newCenter=sum/cluster.size();
	 Put put = new Put(Bytes.toBytes(String.valueOf(key.get())));
	 put.add(Bytes.toBytes("Property"), Bytes.toBytes("newCenter"), Bytes.toBytes(newCenter));
	context.write(null, put);
 }
}

