import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LoadDataTableMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
	private String hbaseTable;	
    private String dataSeperator;
    private String columnFamily1;
    private String columnFamily2;
    private ImmutableBytesWritable hbaseTableName;

    public void setup(Context context) {
        Configuration configuration = context.getConfiguration();		
        hbaseTable = configuration.get("hbase.table.name");		
        dataSeperator = configuration.get("data.seperator");		
        columnFamily1 = configuration.get("COLUMN_FAMILY_1");		
        columnFamily2 = configuration.get("COLUMN_FAMILY_2");		
        hbaseTableName = new ImmutableBytesWritable(Bytes.toBytes(hbaseTable));		
    }

    public void map(LongWritable key, Text value, Context context) {
        try {		
            String[] values = value.toString().split("	");			
            //String rowKey = values[0]+values[5];
            int rowKey = value.hashCode();
            Put put = new Put(Bytes.toBytes(rowKey));			
            put.add(Bytes.toBytes(columnFamily1), Bytes.toBytes("X1"), Bytes.toBytes(values[0]));			
            put.add(Bytes.toBytes(columnFamily1), Bytes.toBytes("X5"), Bytes.toBytes(values[4]));
            put.add(Bytes.toBytes(columnFamily1), Bytes.toBytes("X6"), Bytes.toBytes(values[5]));			
            put.add(Bytes.toBytes(columnFamily1), Bytes.toBytes("Y1"), Bytes.toBytes(values[8]));
            put.add(Bytes.toBytes(columnFamily1), Bytes.toBytes("Y2"), Bytes.toBytes(values[9]));
            put.add(Bytes.toBytes(columnFamily2), Bytes.toBytes("X2"), Bytes.toBytes(values[1]));			
            put.add(Bytes.toBytes(columnFamily2), Bytes.toBytes("X3"), Bytes.toBytes(values[2]));
            put.add(Bytes.toBytes(columnFamily2), Bytes.toBytes("X4"), Bytes.toBytes(values[3]));			
            put.add(Bytes.toBytes(columnFamily2), Bytes.toBytes("X7"), Bytes.toBytes(values[6]));
            put.add(Bytes.toBytes(columnFamily2), Bytes.toBytes("X8"), Bytes.toBytes(values[7]));
            context.write(hbaseTableName, put);			
        } catch(Exception exception) {			
            exception.printStackTrace();			
        }
    }

}
