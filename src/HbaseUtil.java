import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;


public class HbaseUtil {
@SuppressWarnings("unchecked")
public static HashMap LoadFromHtable(String TableName) throws IOException{
	Configuration config=HBaseConfiguration.create();
	HTable currentCenter=new HTable(config, TableName);
	Scan scan = new Scan();
	ResultScanner rs = currentCenter.getScanner(scan); 
	HashMap oldcenter= new HashMap();
	ByteBuffer rowkeyB = ByteBuffer.allocate(1000);
	int Crowkey=0;
	for(Result r : rs){
		System.out.println(r.getRow());
		for (Cell cell : r.listCells()) {
			rowkeyB.put(r.getRow());
			Crowkey=rowkeyB.hashCode();
			oldcenter.put(Crowkey,cell);
			System.out.println("rowkey Int   "+Crowkey);
			System.out.println("columns  "+cell);
		}
	}
	System.out.println(oldcenter);
	return oldcenter;
 }	
}

