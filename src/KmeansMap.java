import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;




public class KmeansMap extends TableMapper<IntWritable, IntWritable> {
	HashMap centerTable;
	Configuration conf =  HBaseConfiguration.create(); 
	Double min=Double.MAX_VALUE;
    Double dis=0.0;
	
		public void setup(Context context) throws IOException,InterruptedException{
		centerTable = HbaseUtil.LoadFromHtable("center");
		//System.out.println("this is map setup");
        super.setup(context);
	   }
	
		public void map(ImmutableBytesWritable rowKey, Result columns, Context context)
				throws IOException, InterruptedException {
			System.out.println("this is map map");
			int TarRowKey=0;
			Iterator it = centerTable.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry pair = (Map.Entry)it.next();
				int Crowkey=(Integer)pair.getKey();
				dis=(Double) Math.sqrt(StrictMath.pow(columns.hashCode() - Crowkey, 2));
	        	 if(dis<min){
	        		 min=dis;
	        		 TarRowKey=Crowkey;
	        	 }
			}
		//	System.out.println("this is loop out");
			context.write(new IntWritable(TarRowKey), new IntWritable(columns.hashCode()));
			//System.out.println(TarRowKey);
}


}
	
