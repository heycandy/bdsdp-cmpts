/**
 * Copyright (c) 2016 chinaSofti.com. All Rights Reserved.
 */
package com.chinasofti.ark.bdadp.component.sample;

import com.chinasofti.ark.bdadp.component.ComponentProps;
import com.chinasofti.ark.bdadp.component.api.Configureable;
import com.chinasofti.ark.bdadp.component.api.RunnableComponent;
import com.chinasofti.ark.bdadp.util.hadoop.HdfsMain;
import com.chinasofti.ark.bdadp.util.hadoop.HiveJdbcUtil;
import com.chinasofti.ark.bdadp.util.io.MyFileFilter;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

/**
 * 数据加载控件
 *
 * @author wgzhang
 * @create 2016-09-13 9:36
 */

public class LoadToHiveRunnableComponent extends RunnableComponent implements Configureable {

  //上传到web服务器/opt/upload文件夹下文件
  private static final String OUT_WEB_PATH = "/opt/uploadToHdfs/";
  //上传到HDFS服务器/user/ark/upload文件夹下文件
  private static final String IN_HDFS_PATH = "/opt/upload/";
  String description = "";
  String dataSource = "";
  String filePath = "";
  String fileName = "";
  String tableName = "";
  String tablePartition = "";
  String param = "";
  String hiveConf = "";
  String
      zkQuorum = "";
  //      "10.100.66.116:24002";
//zkQuorum的"xxx.xxx.xxx.xxx"为集群中Zookeeper所在节点的IP，端口默认是24002
  String userName = "";
  //    "hive_hbase";
  String pwd = "";
  Connection connection = null;
  Map<String, String> mapParam = new HashMap<String, String>();
  String overwriter = null;

  public LoadToHiveRunnableComponent(String id, String name, Logger log) {
    super(id, name, log);
  }

  @Override
  public void configure(ComponentProps props) {
    description = props.getString("descrption");
    dataSource = props.getString("file_source");
    filePath = props.getString("file_path");
    fileName = props.getString("file_name");
    tableName = props.getString("table_name");
    tablePartition = props.getString("table_partition");
    hiveConf = props.getString("hive_conf");
    overwriter = props.getString("overwriter");
//    param = props.getString("component_config_load_to_hive_param");
    if (StringUtils.isEmpty(filePath) || StringUtils.isEmpty(tableName)) {
      throw new RuntimeException("The file path and the table name cannot be empty!");
    }
    if (StringUtils.isEmpty(fileName)) {
      fileName = "*";
    }
    if (StringUtils.isEmpty(String.valueOf(hiveConf))) {
      throw new RuntimeException("hive configuration parameters cannot be empty!");
    }
    String[] pairs = hiveConf.split("&");
    for (int i = 0; i < pairs.length; i++) {
      String[] keyValue = pairs[i].split("=");
      mapParam.put(keyValue[0], keyValue[1]);
    }

    zkQuorum = String.valueOf(mapParam.get("url"));
    userName = String.valueOf(mapParam.get("name"));
    pwd = String.valueOf(mapParam.get("pwd"));

    if (StringUtils.isEmpty(zkQuorum) || StringUtils.isEmpty(userName)) {
      throw new RuntimeException("hive link parameters cannot be empty!");
    }
  }

  @Override
  public void run() {
    debug("LoadToHiveRunnableComponent");

    File dest;
    try {
      if (dataSource.equals("0")) {
        HdfsMain hdfsMain = new HdfsMain();
        File path = new File(filePath);
        if (path.exists()) {
          if (!path.isDirectory()) {
            throw new RuntimeException("The file path must be a folder! ");
          /*InputStream input = new FileInputStream(path);
          //得到目标文件路径（HDFS）
          dest = new File(IN_HDFS_PATH, fileName);
          //文件写入HDFS
          hdfsMain.write(input, dest.getPath());
          //loadtohive
          loadToHive(IN_HDFS_PATH + fileName);*/
          } else {
            MyFileFilter myFile = new MyFileFilter(filePath, fileName);
            String[] fileArr = myFile.fileFilterFun();
            if (fileArr.length <= 0) {
              throw new RuntimeException("File does not exist or the folder is empty!");
            }
            for (int i = 0; i < fileArr.length; i++) {
              //得到源文件（本地）
              File readfile = new File(filePath, fileArr[i]);
              String absolutepath = readfile.getAbsolutePath();//文件的绝对路径
              InputStream input = new FileInputStream(absolutepath);
              //得到目标文件路径（HDFS）
              dest = new File(IN_HDFS_PATH, fileArr[i]);
              //文件写入HDFS
              hdfsMain.write(input, dest.getPath());
              //loadtohive
              loadToHive(IN_HDFS_PATH + fileArr[i]);
            }

          }
        } else {
          throw new RuntimeException("The file path does not exist!");
        }

        /*MyFileFilter myFile = new MyFileFilter(filePath, fileName);
        String[] fileArr = myFile.fileFilterFun();
        for (int i = 0; i < fileArr.length; i++) {

          //得到源文件（本地）
          File source = new File(filePath, fileArr[i]);
          if (source.isDirectory()) {
            for (int j = 0; j < source.list().length; j++) {
              File readfile = new File(source, source.list()[j]);
              String absolutepath = readfile.getAbsolutePath();//文件的绝对路径
              InputStream input = new FileInputStream(absolutepath);
              //得到目标文件路径（HDFS）
              dest = new File(IN_HDFS_PATH, fileArr[j]);
              //文件写入HDFS
              hdfsMain.write(input, dest.getPath());
            }
          }else {
            InputStream input = new FileInputStream(source);
            //得到目标文件路径（HDFS）
            dest = new File(IN_HDFS_PATH, fileName);
            //文件写入HDFS
            hdfsMain.write(input, dest.getPath());
          }
        }
        //loadtohive
        loadToHive(IN_HDFS_PATH + fileName);*/

      } else {
        loadToHive(filePath + "/" + fileName);
      }

    } catch (Exception e) {
      throw new RuntimeException("LoadToHiveRunnableComponent exception: ", e);
    } finally {
      // 关闭JDBC连接
      HiveJdbcUtil.closeConnection(connection);
    }

  }

  /**
   * //loadtohive
   */
  private void loadToHive(String path) {
    String hiveQL = "";
    String tmp_overwriter;
    if ("0".equals(overwriter)) {
      tmp_overwriter = "' OVERWRITE INTO TABLE ";
    } else {
      tmp_overwriter = "' INTO TABLE ";
    }
    if (tablePartition != null && !"".equals(tablePartition)) {
      hiveQL =
          " LOAD DATA INPATH '" + path + tmp_overwriter + tableName + " partition("
          + tablePartition + ");";
    } else {
      hiveQL = " LOAD DATA INPATH '" + path + tmp_overwriter + tableName + ";";
    }
    debug("hiveQL:" + hiveQL);
    try {
      connection = HiveJdbcUtil.getConnection(zkQuorum, userName, "");
      debug("Link to hive");
      HiveJdbcUtil.execHql(connection, "default", hiveQL, 1000);
    } catch (Exception e) {
      throw new RuntimeException("LoadToHiveRunnableComponent exception: ", e);
    }

  }

}

