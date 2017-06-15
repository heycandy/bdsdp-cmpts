package com.chinasofti.ark.bdadp.component;

import com.chinasofti.ark.bdadp.component.api.Configureable;
import com.chinasofti.ark.bdadp.component.api.RunnableComponent;
import com.chinasofti.ark.bdadp.util.hdfs.common.ConfigurationClient;
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
 * Created by Administrator on 2016/9/22.
 */
public class URLDecoder extends RunnableComponent implements Configureable {

  String fileSource = "";
  String filePath = "";
  String destPath = "";
  String destName = "";
  String fileName = "";
  String destSource = "";

  public URLDecoder(String id, String name, Logger log) {
    super(id, name, log);
  }

  @Override
  public void configure(ComponentProps props) {
    fileSource = props.getString("file_source");
    filePath = props.getString("file_path");
    fileName = props.getString("file_name");
    destPath = props.getString("dest_path");
    destName = props.getString("dest_name");
    destSource = props.getString("dest_source");
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
      //输入文件源为local/HDFS
      if ("0".equals(fileSource)) {
        contentDecodeFileFromLocal();
      } else {
        contentDecodeFileFromHDFS();
      }
      info("contentDecode execute successful.");
    } catch (Exception e) {
      error("contentDecode execute failed.", e);
      throw new RuntimeException(e);
    }

  }

  //输入文件源为local
  public void contentDecodeFileFromLocal() {
    try {
      File file = new File(filePath);
      //判断该路径是否合法
      int flag = 0;
      if (!file.exists()) {
        throw new RuntimeException("The file path does not exist!");
      }
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
          FileStream fsm = new FileStream();
          this.urlDecoder(fsm.createInStream(absolutepath, fileSource),
                          fsm.createOutputStream(out_filename, destSource));
          flag = 1;
          info("The" + (i + 1) + "Execute successfully");
        }
        //判断源路径是否合法路径
        if (flag == 0) {
          throw new RuntimeException("There is no matching file for execute in this file path!");
        }
        info("All files run successfully.");
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  //输入文件源为HDFS
  public void contentDecodeFileFromHDFS() {
    try {
      //fs文件系统
      FileSystem fs = FileSystem.get(ConfigurationClient.getInstance().getConfiguration());
      Path dir = new Path(filePath);

      //判断该路径是否合法
      int flag = 0;
      if (!fs.exists(dir)) {
        throw new RuntimeException("The file path does not exist!");
      }
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
          FileStream fsm = new FileStream();
          this.urlDecoder(fsm.createInStream(inputHDFSpath, fileSource),
                          fsm.createOutputStream(out_filename, destSource));
          flag = 1;
          info("The" + (i + 1) + "Execute successfully");
        }
        //判断源路径是否合法路径
        if (flag == 0) {
          throw new RuntimeException("There is no matching file for execute in this file path!");
        }
        info("All files run successfully.");
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  //URL解码处理过程
  private void urlDecoder(InputStream in, OutputStream out) throws IOException {
    String str;
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));

    while ((str = reader.readLine()) != null) {
      str = java.net.URLDecoder.decode(str, "utf-8");
      writer.write(str + "\n");
    }
    reader.close();
    writer.close();
  }
}
