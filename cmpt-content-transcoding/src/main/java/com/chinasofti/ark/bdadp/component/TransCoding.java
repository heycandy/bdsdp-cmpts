package com.chinasofti.ark.bdadp.component;

import com.chinasofti.ark.bdadp.component.api.Configureable;
import com.chinasofti.ark.bdadp.component.api.RunnableComponent;
import com.chinasofti.ark.bdadp.util.hdfs.common.ConfigurationClient;
import com.chinasofti.ark.bdadp.util.io.EncodingDetect;
import com.chinasofti.ark.bdadp.util.io.FileStream;
import com.chinasofti.ark.bdadp.util.io.MyFileFilter;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

/**
 * 编码转换 Created by TongTong on 2016/9/8.
 */
public class TransCoding extends RunnableComponent implements Configureable {

  String fileSource = "";
  String filePath = "";
  String fileName = "";
  String fileEncode = "";
  String destSource = "";
  String destPath = "";
  String destName = "";
  String destEncode = "UTF-8";


  public TransCoding(String id, String name, Logger log) {
    super(id, name, log);
  }

  @Override
  public void configure(ComponentProps props) {
    fileSource = props.getString("file_source");
    filePath = props.getString("file_path");
    fileName = props.getString("file_name");
    fileEncode = props.getString("file_encode");
    destSource = props.getString("dest_source");
    destPath = props.getString("dest_path");
    destName = props.getString("dest_name");
    //源文件路径和目标文件路径不能为空
    if (StringUtils.isBlank(filePath) || StringUtils.isBlank(destPath)) {
      throw new RuntimeException("The source path or the dest path can not be empty!");
    }
    //源文件名称为空，默认匹配*
    if (StringUtils.isBlank(fileName)) {
      fileName = "*";
    }
    //判断输出路径文件夹是否存在，不存在就自动创建文件夹
    File tempDir = new File(destPath);
    if (!tempDir.exists()) {
      tempDir.mkdirs();
    }
    debug("Output folder automatically created successfully!");
  }

  @Override
  public void run() {
    try {
      if ("0".equals(fileEncode)) {
        fileEncode = "UTF-16";
      } else if ("1".equals(fileEncode)) {
        fileEncode = "GBK";
      } else if ("2".equals(fileEncode)) {
        fileEncode = "GB2312";
      }
      //输入文件源为local/HDFS
      if ("0".equals(fileSource)) {
        transCodingFileFromLocal();
      } else {
        transCodingFileFromHDFS();
      }
    } catch (Exception e) {
      error("TransCoding execute failed.", e);
      throw new RuntimeException(e);
    }
  }

  //输入文件源为Local
  public void transCodingFileFromLocal() {
    try {
      File file = new File(filePath);
      //判断该路径是否合法
      int flag = 0;
      if (!file.exists()) {
        throw new RuntimeException("The file path does not exist!");
      }
      //当前路径不是一个文件夹
      if (!file.isDirectory()) {
        debug("This is not a folder.");
        throw new RuntimeException("The source file path must be a folder!");
      } else {
        debug("This is a folder.");
        //查询通配符文件名
        MyFileFilter mm = new MyFileFilter(filePath, fileName);
        String[] m_filerName = mm.fileFilterFun();
        if (null == m_filerName || m_filerName.length <= 0) {
          throw new RuntimeException("The source file is null！");
        }
        for (int i = 0; i < m_filerName.length; i++) {
          File readfile = new File(filePath + File.separator + m_filerName[i]);
          String absolutepath = readfile.getAbsolutePath();//文件的绝对路径
          String filename = readfile.getName();//读到的文件名
          String out_filename = destPath + File.separator + filename;
          //////// 开始挨个的读取文件  ////////
          String code = EncodingDetect.getJavaEncode(absolutepath);
          if (code.equals("Unicode")) {
            code = "UTF-16";
          }
          if (!code.equals(fileEncode)) {
            throw new RuntimeException("This file format does not match!");
          } else {
            FileStream fsm = new FileStream();
            this.transCode(fsm.createInStream(absolutepath, fileSource),
                           fsm.createOutputStream(out_filename, destSource), fileEncode,
                           destEncode);
            info("The" + (i + 1) + "Execute successfully");
            flag = 1;
          }
        }
        //判断源路径是否合法路径
        if (flag == 0) {
          throw new RuntimeException("There is no matching file for execute in this file path!");
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    info("All files run successfully.");
  }

  //输入文件源为HDFS
  public void transCodingFileFromHDFS() {
    try {
      //fs文件系统
      FileSystem fs = FileSystem.get(ConfigurationClient.getInstance().getConfiguration());
      Path dir = new Path(filePath);
      //判断该路径是否合法
      int flag = 0;
      if (!fs.exists(dir)) {
        throw new RuntimeException("The file path does not exist!");
      }
      //当前路径不是一个文件夹
      if (!fs.isDirectory(dir)) {
        debug("This is not a folder.");
        throw new RuntimeException("The source file path must be a folder!");
      } else if (fs.isDirectory(dir)) {
        debug("This is a folder.");
        //查询通配符文件名
        FileStatus[] status = fs.globStatus(new Path(filePath, fileName));
        if (null == status || status.length <= 0) {
          throw new RuntimeException("The source file is null！");
        }
        for (int i = 0; i < status.length; i++) {
          File readfile = new File(status[i].getPath().toString());
          String inputHDFSpath = status[i].getPath().toString();//hdfs文件的输入路径
          String filename = readfile.getName();//读到的文件名
          String out_filename = destPath + File.separator + filename;
          //////// 开始挨个的读取文件  ////////
          String code = EncodingDetect.getJavaEncodeHDFS(fs, status[i], inputHDFSpath);
          if (code.equals("Unicode")) {
            code = "UTF-16";
          }
          if (!code.equals(fileEncode)) {
            throw new RuntimeException("This file format does not match!");
          } else {
            FileStream fsm = new FileStream();
            this.transCode(fsm.createInStream(inputHDFSpath, fileSource),
                           fsm.createOutputStream(out_filename, destSource), fileEncode,
                           destEncode);
            info("The" + (i + 1) + "Execute successfully");
            flag = 1;
          }
        }
        //判断源路径是否合法路径
        if (flag == 0) {
          throw new RuntimeException("There is no matching file for execute in this file path!");
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    info("All files run successfully.");
  }

  //编码转换
  private void transCode(InputStream in, OutputStream out, String fileEncode, String destEncode)
      throws IOException {

    BufferedReader br = new BufferedReader(new InputStreamReader(in, fileEncode));
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out, destEncode));
    int ch = 0;
    while ((ch = br.read()) != -1) {
      bw.write(ch);
    }
    br.close();
    bw.close();
  }
}
