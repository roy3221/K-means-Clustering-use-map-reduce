import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.KeyValue;


public class initialTables {
	
	public void inputTable() throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
		Configuration config = HBaseConfiguration.create();
		@SuppressWarnings("resource")
		HBaseAdmin hbAdmin = new HBaseAdmin(config);
		@SuppressWarnings("deprecation")
		HTableDescriptor hbHtd = new HTableDescriptor("input");
		HColumnDescriptor CF_1 = new HColumnDescriptor("Area");
		HColumnDescriptor CF_2 = new HColumnDescriptor("Property");
		hbHtd.addFamily(CF_1);
		hbHtd.addFamily(CF_2);
		hbAdmin.createTable(hbHtd);
	}
	@SuppressWarnings({ "deprecation", "resource", "unused" })
	public void center(HTable htable, int kValue) throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
		Configuration config = HBaseConfiguration.create();
		HBaseAdmin hbAdmin = new HBaseAdmin(config);
		HTableDescriptor hbHtd = new HTableDescriptor("center");
		HColumnDescriptor CF_1 = new HColumnDescriptor("Area");
		HColumnDescriptor CF_2 = new HColumnDescriptor("Property");
		hbHtd.addFamily(CF_1);
		hbHtd.addFamily(CF_2);
		hbAdmin.createTable(hbHtd);
		HTable center= new HTable(config,"center");
		  HColumnDescriptor[] columnFamilies = center.getTableDescriptor().getColumnFamilies();
		Scan scan = new Scan();
        ResultScanner rs = null;       
        try {
        	 int i=0;
            rs = htable.getScanner(scan);
            //Result r=new Result();
            int row[]=new int[kValue];
            int k=0;
            int count=0;
            int flag = 0;
  		  while(count<kValue) {
  			Random rand = new Random();
  			k=rand.nextInt(768);
  			 for(int j=0;j<count;j++){
                 if(row[j]==k){
                     flag = 1;
                     break;
                 }else{
                     flag = 0;
                 }
             }
             if(flag==0){
                 row[count] = k;
                 count++;
             }
  		  }
  		  for(int x=0;x<kValue;x++){
  			 System.out.println(row[x]);
  		  }
  		 
  		for ( Result r : rs) {
  			++i;
  			for(int m =0; m<kValue; m++) {
  			if(i==row[m]){ 
        	 for (Cell cell : r.listCells()) {
            			 Put put = new Put(r.getRow());
            			put.add(cell);
            			 center.put(put);
            		 }
        		 }          			 
        	 }
        }
            	
            }finally {
                rs.close();
            }

	}
}
