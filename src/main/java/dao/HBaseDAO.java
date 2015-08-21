package dao;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author 陈苗 E-mail:510473319@qq.com
 * @version 创建时间：Aug 13, 2015 11:48:13 AM
 * 类说明
 */
public class HBaseDAO {
	static Configuration conf=HBaseConfiguration.create();
	static{
		//zookeeper地址
		conf.set("hbase.zookeeper.quorum", "192.168.1.117");
		//zookeeper端口
		conf.set("hbase.zookeeper.property.clientPort", "2181");
	}
	
	public static void createTable(String tablename,String columnFamily) throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
		HBaseAdmin admin=new HBaseAdmin(conf);
		if(admin.tableExists(tablename)){
			System.out.println("Table exists!");
		}else{
			HTableDescriptor tableDesc=new HTableDescriptor(TableName.valueOf(tablename));
			tableDesc.addFamily(new HColumnDescriptor(columnFamily));
			admin.createTable(tableDesc);
			System.out.println("create table success!");
		}
		admin.close();
	}
	
	public static boolean deleteTable(String tablename) throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
		HBaseAdmin admin=new HBaseAdmin(conf);
		if(admin.tableExists(tablename)){
			try {
				admin.disableTable(tablename);
				admin.deleteTable(tablename);
			} catch (Exception e) {
				e.printStackTrace();
				admin.close();
				return false;
			}
		}
		admin.close();
		return true;
	}
	
	public static void putRow(HTable table,String rowKey,String columnFamily,String key,String value) throws IOException{
		Put rowPut=new Put(Bytes.toBytes(rowKey));
		rowPut.add(columnFamily.getBytes(),key.getBytes(),value.getBytes());
		table.put(rowPut);
		System.out.println("put '"+rowKey+"', '"+columnFamily+":"+key+"', '"+value+"'");
	}
	
	public static Result getRow(HTable table,String rowKey) throws IOException{
		Get get=new Get(Bytes.toBytes(rowKey));
		Result result=table.get(get);
		System.out.println("Get: "+result);
		return result;
	}
	
	public static void deleteRow(HTable table,String rowKey) throws IOException{
		Delete delete=new Delete(Bytes.toBytes(rowKey));
		table.delete(delete);
		System.out.println("Delete row:"+rowKey);
	}
	
	public static ResultScanner scanAll(HTable table) throws IOException{
		Scan s=new Scan();
		ResultScanner rs=table.getScanner(s);
		return rs;
	}
	
	public static ResultScanner scanRange(HTable table,String startrow,String endrow) throws IOException{
		Scan s=new Scan(Bytes.toBytes(startrow),Bytes.toBytes(endrow));
		ResultScanner rs=table.getScanner(s);
		return rs;
	}
	
	public static ResultScanner scanFilter(HTable table,String startrow,Filter filter) throws IOException{
		Scan s=new Scan(Bytes.toBytes(startrow),filter);
		ResultScanner rs=table.getScanner(s);
		return rs;
	}
	
	public static void main(String[] args) throws IOException{
		HTable table = new HTable(conf, "tab1");
        
//      ResultScanner rs = HBaseDAO.scanRange(table, "2015-08-19*", "2015-08-21*");
//    	ResultScanner rs = HBaseDAO.scanRange(table, "100001", "100004");
//    	ResultScanner rs = HBaseDAO.scanAll(table);
//
//    	for(Result row:rs) {
//    		System.out.println("Row: "+new String(row.getRow()));
//    		for(Entry<byte[], byte[]> entry:row.getFamilyMap("fam1".getBytes()).entrySet()){
//    			String key=new String(entry.getKey());
//    			String value=new String(entry.getValue());
//    			System.out.format("COLUMN \t fam1:%s \t %s \n", key,value);
//    		}
//    	}
    	
		HBaseDAO.createTable("tab1", "fam1");
//		HBaseDAO.putRow(table, "row2", "fam1", "col4", "val3");
//    	HBaseDAO.deleteRow(table, "100001");
//    	HBaseDAO.getRow(table, "100003");
//    	HBaseDAO.deleteTable("tab1");
    	table.close();
	}
}
