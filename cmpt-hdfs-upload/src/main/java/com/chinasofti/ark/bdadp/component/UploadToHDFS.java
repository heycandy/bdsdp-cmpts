package com.chinasofti.ark.bdadp.component;

import com.chinasofti.ark.bdadp.component.ComponentProps;
import com.chinasofti.ark.bdadp.component.api.Configureable;
import com.chinasofti.ark.bdadp.component.api.RunnableComponent;
import com.chinasofti.ark.bdadp.util.hdfs.common.ConfigurationClient;
import com.chinasofti.ark.bdadp.util.io.FileStream;
import com.chinasofti.ark.bdadp.util.io.MyFileFilter;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by Administrator on 2016/11/17.
 */
public class UploadToHDFS extends RunnableComponent implements Configureable {

  public static int BUFFER_SIZE = 2048;
  String resource = "";
  String filePath = "";
  String fileName = "";
  String destSource = "";
  String destPath = "";

  public UploadToHDFS(String id, String name, Logger log) {
    super(id, name, log);
  }

  @Override
  public void configure(ComponentProps props) {
    resource = props.getString("file_source");
    filePath = props.getString("file_path");
    fileName = props.getString("file_name");
    destSource = props.getString("dest_source");
    destPath = props.getString("dest_path");

    //源文件路径和目标文件路径不能为空
    if (StringUtils.isBlank(filePath) || StringUtils.isBlank(destPath)) {
      throw new RuntimeException("The source path or the dest path can not be empty!");
    }
    if (StringUtils.isBlank(fileName)) {
      fileName = "*";
    }
    //判断输出路径文件夹是否存在，不存在就自动创建文件夹
    File tempDir = new File(destPath);
    if (!tempDir.exists()) {
      tempDir.mkdirs();
    }
  }

  @Override
  public void run() {
    //Local文件上传到HDFS
    try {
      if ("0".equals(resource)) {
        loadFromLocal();
      } else {
        loadFromHDFS();
      }
    } catch (Exception e) {
      error("LoadToHDFS execute failed.", e);
      throw new RuntimeException(e);
    }
  }

  private void loadFromLocal() throws IOException {
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
        FileStream fsm = new FileStream();
        this.fileStore(fsm.createInStream(absolutepath, resource),
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
  }

  private void loadFromHDFS() throws IOException {
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
    } else {
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
        this.fileStore(fsm.createInStream(inputHDFSpath, resource),
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
  }

  private void fileStore(InputStream in, OutputStream out) throws IOException {
    IOUtils.copyBytes(in, out, BUFFER_SIZE, false);
    if (in != null) {
      in.close();
      out.close();
    }
  }
}
