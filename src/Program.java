import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.rest.Constants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

@SuppressWarnings("deprecation")
public class Program  {	
	public static enum counter {
		runs
	}
    private static final String DATA_SEPERATOR = "	";	
    private static final String INPUT_TABLE_NAME = "input";	
    private static final String CENTER_TABLE_NAME = "center";	
    private static final String COLUMN_FAMILY_1="Area";	
    private static final String COLUMN_FAMILY_2="Property";	
    static HTable inputTable;
    public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException, InterruptedException, ClassNotFoundException {
    	// to check if there are tables in HBase, if yes, delete them.
    	HBaseConfiguration hbaseConfig=null; 
    	Configuration configd = HBaseConfiguration.create();    
    	hbaseConfig=new HBaseConfiguration(configd);  
    	HBaseAdmin admin = new HBaseAdmin(hbaseConfig);  
        if(admin.isTableAvailable(INPUT_TABLE_NAME)){
        	admin.disableTable(INPUT_TABLE_NAME);  
            admin.deleteTable(INPUT_TABLE_NAME);
        }
        
        if(admin.isTableAvailable(CENTER_TABLE_NAME)){
        	 admin.disableTable(CENTER_TABLE_NAME);  
             admin.deleteTable(CENTER_TABLE_NAME);
        }
       
    	
    	 new initialTables().inputTable();
         String outputPath = "output";
         Configuration configuration1 = HBaseConfiguration.create();		
         configuration1.set("data.seperator", DATA_SEPERATOR);		
         configuration1.set("hbase.table.name",INPUT_TABLE_NAME);		
         configuration1.set("COLUMN_FAMILY_1",COLUMN_FAMILY_1);		
         configuration1.set("COLUMN_FAMILY_2",COLUMN_FAMILY_2);	
         HTable DataTable= new HTable(configuration1,INPUT_TABLE_NAME);
         Job job1 = new Job(configuration1);		
         job1.setJarByClass(Program.class);		
         job1.setJobName("Bulk Loading HBase Table::"+INPUT_TABLE_NAME);		
         job1.setInputFormatClass(TextInputFormat.class);		
         job1.setMapOutputKeyClass(ImmutableBytesWritable.class);		
         job1.setMapperClass(LoadDataTableMapper.class);		
         FileInputFormat.addInputPaths(job1, args[0]);		
         FileSystem.getLocal(configuration1).delete(new Path(outputPath), true);		
         FileOutputFormat.setOutputPath(job1, new Path(outputPath));		
         job1.setMapOutputValueClass(Put.class);
         HFileOutputFormat.configureIncrementalLoad(job1, DataTable);
         job1.waitForCompletion(true);
         inputTable=DataTable;
         
        if (job1.isSuccessful()) {
     	   HbaseBulkLoad.doBulkLoad(outputPath,INPUT_TABLE_NAME);
     	   new initialTables().center(inputTable,10);
     	   Configuration config = HBaseConfiguration.create();
     	   Job job2 = new Job(config);
     	   job2.setJarByClass(Program.class);		
           job2.setJobName("Kmeans");	

     	   Scan scan = new Scan();
     	   scan.setCaching(500);        
     	   scan.setCacheBlocks(false);  
     	   TableMapReduceUtil.initTableMapperJob(
     		INPUT_TABLE_NAME,        // input table
     	     scan,               // Scan instance to control CF and attribute selection
     	     KmeansMap.class,     // mapper class
     	    IntWritable.class,         // mapper output key
     	   IntWritable.class,  // mapper output value
     	     job2);
     	   job2.setReducerClass(KmeansReduce.class);
     	   job2.setOutputKeyClass(IntWritable.class);
     	   job2.setOutputValueClass(IntWritable.class);
     	 TableMapReduceUtil.initTableReducerJob(
     	     "center",        // output table
     	     null,    // reducer class
     	     job2);
     	   boolean b = job2.waitForCompletion(true);
     	   if (!b) {
     	     throw new IOException("error with job2!");
     	   }
     	   
         } 

        /*try {
            int response = ToolRunner.run(HBaseConfiguration.create(), new Program(), args);			
            if(response == 0) {				
                System.out.println("Job is successfully completed...");
            } else {
                System.out.println("Job failed...");
            }
        } catch(Exception exception) {
            exception.printStackTrace();
        }*/
     
        
        
    }
}
