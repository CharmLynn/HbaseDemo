package com.youku.data;

/**
 * Created by asha on 16-4-20.
 */

import java.io.*;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class BrushDetect {
    public static HashMap<String, HashMap> readHDFSListAll(String hdfsdir) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(hdfsdir), conf);
        FileStatus fileList[] = fs.listStatus(new Path(hdfsdir));

        HashMap<String,HashMap> hm = new HashMap<String, HashMap>();
        HashMap<Integer,Integer> hmpv = new HashMap<Integer, Integer>();
        HashMap<Integer,Integer> hmurl = new HashMap<Integer, Integer>();


        for (int i = 0; i < fileList.length; i++) {

            Path p = new Path( fileList[i].getPath().toString() );
            InputStream in = fs.open( p );
            //BufferedReader包装一个流
            BufferedReader buff = new BufferedReader( new InputStreamReader( in ) );
            String str = null;
            while ((str = buff.readLine()) != null) {
//                System.out.println( str );
                String key = str.trim().split( "\t" )[0];
                int valuekey = Integer.valueOf( str.trim().split( "\t" )[1] ).intValue();
                int valuevalue = Integer.valueOf( str.trim().split( "\t" )[2] ).intValue();
                if(hm.containsKey( key )){
                    HashMap<Integer,Integer> hmvalue = hm.get( key );
                    hmvalue.put( valuekey,valuevalue );
                    hm.put( key,hmvalue );
                }
                else{
                    HashMap<Integer,Integer> hmvalue = new HashMap<Integer, Integer>(  );
                    hmvalue.put( valuekey,valuevalue );
                    hm.put( key,hmvalue );
                }

            }

            buff.close();
            in.close();
            }
        fs.close();
        return hm;
    }

    public static void main(String[] args) throws Exception {
        Calendar cale = Calendar.getInstance();
        Date tasktime=cale.getTime();
        SimpleDateFormat dateform=new SimpleDateFormat("yyyyMMdd");

        HashMap<String, HashMap> mytest = readHDFSListAll( "hdfs://asha:9000/user/asha/" + dateform  );
        System.out.println(mytest);
    }
}
