package com.youku.data;

/**
 * Created by asha on 16-4-27.
 */
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.apache.hadoop.hbase.util.Bytes.toString;
import static org.apache.hadoop.hbase.util.Bytes.toStringBinary;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class HbaseDemo {

//    private  static Configuration conf = null;


//    static {
//        conf = HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.quorum", "localhost");
//        conf.set("hbase.zookeeper.property.clientPort", "2181");
//    }

    //create a table
    public static void createTable(String tablename, String[] columnFaimliys) throws IOException {
        Configuration conf = HBaseConfiguration.create();

        HBaseAdmin hadmin = new HBaseAdmin(conf);

        HTableDescriptor tabledec = null;
        if (hadmin.tableExists(tablename)) {
            System.out.println("Table " + tablename + " has exists");
            System.exit(0);
        } else {
            tabledec = new HTableDescriptor(TableName.valueOf(toBytes(tablename)));
            for (String columnFamily : columnFaimliys) {
                tabledec.addFamily(new HColumnDescriptor(columnFamily));
            }
        }
        hadmin.createTable(tabledec);
        System.out.println("Table " + tablename + " create success!");
    }


    //delete a table
    public  static void deleteTable(String tablename) throws IOException{
        Configuration conf = HBaseConfiguration.create();

        HBaseAdmin hadmin = new HBaseAdmin(conf);
        if (hadmin.tableExists(tablename)) {
            hadmin.disableTable(tablename);
            hadmin.deleteTable(tablename);
            System.out.println("Table " + tablename + " has deleted");
        }else{
            System.out.println("Table " + tablename + " is not exits");
            System.exit(0);
        }
    }

    // add one row
    public static void addRow(String tablename,String rowkey,String columnFamily,String column ,String value) throws IOException {
        Configuration conf = HBaseConfiguration.create();

        HTable table = new HTable(conf,tablename);
        Put put = new Put(toBytes(rowkey));
        put.add(toBytes(columnFamily), toBytes(column), toBytes(value));
        table.put(put);
    }

    //delete one row
    public static void delRow(String tablename,String rowkey) throws  IOException{
        Configuration conf = HBaseConfiguration.create();

        HTable table = new HTable(conf,tablename);
        Delete del = new Delete(toBytes(rowkey));
        table.delete(del);
    }

    //delete severl rows
    public static void delRows(String tablename, String[] rowkeys) throws IOException {
        Configuration conf = HBaseConfiguration.create();

        HTable table = new HTable(conf,tablename);
        List<Delete> delList = new ArrayList<Delete>();
        for (String rowkey: rowkeys){
            Delete del = new Delete(toBytes(rowkey));
            delList.add(del);
        }
        table.delete(delList);
    }

    //get one row data
    public static void getRow(String tablename,String rowkey) throws IOException{
        Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf,tablename);
        Get get = new Get(toBytes(rowkey));
        Result result = table.get(get);

        for(Cell cell : result.rawCells()) {
            System.out.println("RowKey: " + toStringBinary(result.getRow()) + "\t" + toStringBinary(CellUtil.cloneQualifier(cell))
                            + " Value: " + toStringBinary(CellUtil.cloneValue(cell)));
        }

    }

    //get all data

    public  static  void  getAllRow(String tablename) throws  IOException{
        Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf,tablename);
        Scan scan = new Scan();
        ResultScanner results = table.getScanner(scan);

        for (Result result : results){
            for (Cell cell : result.rawCells()){
                System.out.println("RowKey: " + toStringBinary(result.getRow()) + toStringBinary(CellUtil.cloneQualifier(cell))
                        + " Value: " + toStringBinary(CellUtil.cloneValue(cell)));
            }

        }

    }


    // read from hdfs
    public static void readandput(String dateform,String tablename,String sourcedir) throws Exception {
        Configuration conf = new Configuration();
//        String hdfsdir = "hdfs://asha:9000/user/asha/" + dateform;
        String hdfsdir = sourcedir + dateform;

        FileSystem fs = FileSystem.get(URI.create(hdfsdir), conf);
        FileStatus fileList[] = fs.listStatus(new Path(hdfsdir));


        for (int i = 0; i < fileList.length; i++) {

            Path p = new Path( fileList[i].getPath().toString() );
            InputStream in = fs.open( p );
            //BufferedReader包装一个流
            BufferedReader buff = new BufferedReader( new InputStreamReader( in ) );
            String str = null;
            while ((str = buff.readLine()) != null) {
//                System.out.println( str );
                String[] tmpvalue = str.trim().split("\t");
                String rowkey = dateform + "_" + tmpvalue[1];
                String column = tmpvalue[0];
                String value = tmpvalue[2];
                addRow(tablename,rowkey,"paraFamiliy",column,value);
        }
            buff.close();
            in.close();
    }
        fs.close();
    }

    public static void main(String[] args) throws Exception {
        HbaseDemo hdemo = new HbaseDemo();
//        String[] columnfaimiles = {"scool","classroom"};
//        createTable("mytable",columnfaimiles);
//        deleteTable("mytable");
//        addRow("mytable","haihei","classroom","15423","99");

//        Date now = new Date();
//        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
//        String strDate = dateFormat.format( now );
//        System.out.println(strDate);
//        readandput("20160503","brushtable","hdfs://asha:9000/user/asha/");
        getRow("brushtable","20160503_3870");
    }
}

