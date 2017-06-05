/**
 * Copyright (c) 2016 chinaSofti.com. All Rights Reserved.
 */
package com.chinasofti.ark.bdadp.component.sample;

import com.chinasofti.ark.bdadp.component.ComponentProps;
import com.chinasofti.ark.bdadp.component.api.Configureable;
import com.chinasofti.ark.bdadp.component.api.RunnableComponent;
import com.chinasofti.ark.bdadp.util.hdfs.common.ConfigurationClient;
import com.chinasofti.ark.bdadp.util.io.FileStream;
import com.chinasofti.ark.bdadp.util.io.MyFileFilter;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * 文件解压控件
 */
public class FileDecompressionRunnableComponent extends RunnableComponent implements Configureable {

  public static int BUFFER_SIZE = 2048;
  String resource = "";
  String filePath = "";
  String fileName = "";
  String fileType = "";
  String destName = "";
  String destPath = "";
  String destSource = "";

  public FileDecompressionRunnableComponent(String id, String name, Logger log) {
    super(id, name, log);
  }

  private static String toPath(String pathStr) {
    return pathStr.endsWith(File.separator) ? pathStr : pathStr + File.separator;
  }

  @Override
  public void configure(ComponentProps props) {
    resource = props.getString("file_source");
    filePath = props.getString("file_path");
    fileName = props.getString("file_name");
    fileType = props.getString("file_type");
    destName = props.getString("dest_name");
    destPath = props.getString("dest_path");
    destSource = props.getString("dest_source");

    //源文件路径和目标文件路径不能为空
    if (StringUtils.isBlank(filePath) || StringUtils.isBlank(destPath)) {
      throw new RuntimeException("The source path or the dest path can not be empty！");
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
      if ("0".equals(resource)) {
        decompressFileFromLocal();
      } else {
        decompressFileFromHDFS();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  //输入源为local
  public void decompressFileFromLocal() {
    try {
      File file = new File(filePath);
      //判断该路径是否合法
      if (!file.exists()) {
        throw new RuntimeException("The file path does not exist");
      }
      int flag = 0;
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
          throw new RuntimeException("The source file is null!");
        }
        String fileTypeSuffix = "";
        for (int i = 0; i < m_filerName.length; i++) {
          File readfile = new File(filePath + File.separator + m_filerName[i]);
          String absolutepath = readfile.getAbsolutePath();//文件的绝对路径
          //////// 开始挨个的读取文件  ////////
          FileStream fsm = new FileStream();
          InputStream in = fsm.createInStream(absolutepath, resource);
          //每个文件的后缀名
          String fileSuffix = suffixMatch(absolutepath);
          if ("0".equals(fileType)) {
            fileTypeSuffix = "zip";
            if (fileTypeSuffix.equals(fileSuffix)) {
              unZip(in, destPath);
              flag = 1;
              info("The" + (i + 1) + "Execute successfully");
            }
          } else if ("1".equals(fileType)) {
            fileTypeSuffix = "tar";
            if (fileTypeSuffix.equals(fileSuffix)) {
              unTar(in, destPath);
              flag = 1;
              info("The" + (i + 1) + "Execute successfully");
            }
          } else if ("2".equals(fileType)) {
            fileTypeSuffix = "gz";

            if (fileTypeSuffix.equals(fileSuffix)) {
              //获取输出文件名
              File src = new File(absolutepath);
              String destName = src.getName().substring(0, src.getName().lastIndexOf("."));
              unGz(in, destPath, destName);
              flag = 1;
              info("The" + (i + 1) + "Execute successfully");
            }
          }
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

  //输入源为HDFS
  public void decompressFileFromHDFS() {
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
        String fileTypeSuffix = "";
        for (int i = 0; i < status.length; i++) {
          File readfile = new File(status[i].getPath().toString());
          String inputHDFSpath = status[i].getPath().toString();//hdfs文件的输入路径
          String filename = readfile.getName();//读到的文件名
          //////// 开始挨个的读取文件  ////////
          FileStream fsm = new FileStream();
          InputStream in = fsm.createInStream(inputHDFSpath, resource);
          //每个文件的后缀名
          String fileSuffix = suffixMatch(inputHDFSpath);
          if ("0".equals(fileType)) {
            fileTypeSuffix = "zip";
            if (fileTypeSuffix.equals(fileSuffix)) {
              unZip(in, destPath);
              flag = 1;
              info("The" + (i + 1) + "Execute successfully");
            } else {
              //文件格式不匹配，继续读取文件夹下的下一个目录文件
              continue;
            }
          } else if ("1".equals(fileType)) {
            fileTypeSuffix = "tar";
            if (fileTypeSuffix.equals(fileSuffix)) {
              unTar(in, destPath);
              flag = 1;
              info("The" + (i + 1) + "Execute successfully");
            } else {
              //文件格式不匹配，继续读取文件夹下的下一个目录文件
              continue;
            }
          } else if ("2".equals(fileType)) {
            fileTypeSuffix = "gz";

            if (fileTypeSuffix.equals(fileSuffix)) {
              //获取输出文件名
              File src = new File(inputHDFSpath);
              String destName = src.getName().substring(0, src.getName().lastIndexOf("."));
              unGz(in, destPath, destName);
              flag = 1;
              info("The" + (i + 1) + "Execute successfully");
            } else {
              //文件格式不匹配，继续读取文件夹下的下一个目录文件
              continue;
            }
          }
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

  //判断文件格式,返回.后面的文件后缀名
  private String suffixMatch(String filePath) {
    try {
      File f = new File(filePath);
      String fileName = f.getName();
      String suffix = fileName.substring(fileName.lastIndexOf(".") + 1);
      suffix = suffix.toLowerCase();
      return suffix;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void unZip(InputStream in, String destDir) throws IOException {
    destDir = toPath(destDir);
    createDirectory(toPath(destDir));

    ZipArchiveInputStream zipIn = null;
    ZipArchiveEntry entry = null;
    zipIn = new ZipArchiveInputStream(new BufferedInputStream(in, BUFFER_SIZE));
    List<String> fileNames = new ArrayList<String>();

    while ((entry = zipIn.getNextZipEntry()) != null) {
      fileNames.add(entry.getName());
      if (entry.isDirectory()) {
        createDirectory(toPath(destDir) + entry.getName()); //创建空目录
      } else {
        createDirectory(toPath(destDir)); //创建输出目录
        FileStream fsm = new FileStream();
        OutputStream out = fsm.createOutputStream(toPath(destPath) + entry.getName(), destSource);
        OutputStream os = new BufferedOutputStream(out, BUFFER_SIZE);
        IOUtils.copy(zipIn, os);
        os.flush();
        os.close();
      }
    }
  }

  private void unTar(InputStream in, String destDir) throws IOException {
    destDir = toPath(destDir);

    TarArchiveInputStream tarIn = null;
    TarArchiveEntry entry = null;
    List<String> fileNames = new ArrayList<String>();

    tarIn = new TarArchiveInputStream(in, BUFFER_SIZE);
    while ((entry = tarIn.getNextTarEntry()) != null) {
      fileNames.add(entry.getName());
      if (entry.isDirectory()) {//是目录
        createDirectory(toPath(destDir) + entry.getName());//创建空目录
      } else {//是文件
        createDirectory(toPath(destDir)); //创建输出目录
        OutputStream out = null;

        FileStream fsm = new FileStream();
        out =
            new BufferedOutputStream(
                fsm.createOutputStream(toPath(destDir) + entry.getName(), destSource));
        int length = 0;
        byte[] b = new byte[2048];
        while ((length = tarIn.read(b)) != -1) {
          out.write(b, 0, length);
        }
        out.flush();
        out.close();
      }
    }
  }

  private void unGz(InputStream in, String destDir, String destName) throws IOException {
    destDir = toPath(destDir);
    createDirectory(toPath(destDir));

    GzipCompressorInputStream gzipIn = null;
    gzipIn = new GzipCompressorInputStream(new BufferedInputStream(in, BUFFER_SIZE));

    OutputStream os = null;
    FileStream fsm = new FileStream();
    os =
        new BufferedOutputStream(
            fsm.createOutputStream(toPath(toPath(destPath) + destName), destSource), BUFFER_SIZE);

    IOUtils.copy(gzipIn, os);
    os.flush();
    os.close();
    gzipIn.close();
  }

  private void createDirectory(String dir) {
    new File(dir).mkdirs();
  }
}
