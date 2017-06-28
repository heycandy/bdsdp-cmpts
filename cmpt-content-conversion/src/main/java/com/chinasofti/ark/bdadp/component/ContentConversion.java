/**
 * Copyright (c) 2016 chinaSofti.com. All Rights Reserved.
 */
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 内容转换
 *
 * @author wgzhang
 * @create 2016-09-21 20:00
 */
public class ContentConversion extends RunnableComponent
    implements Configureable {

  String contentSource = "";
  String contentPath = "";
  String contentName = "";
  String destSource = "";
  String lineSperator = "";
  String columnSperator = "";
  String charReplaceSrc = "";
  String charReplaceDest = "";
  String trim = "";
  String destPath = "";
  String defaultCharSet = "UTF-8";
  String destName = "";

  public ContentConversion(String id, String name, Logger log) {
    super(id, name, log);
  }

  @Override
  public void configure(ComponentProps props) {
    contentSource = props.getString("file_source");
    contentPath = props.getString("file_path");
    contentName = props.getString("file_name");
    destSource = props.getString("dest_source");
    lineSperator = props.getString("line_sperator");
    columnSperator = props.getString("column_sperator");
    charReplaceSrc = props.getString("char_replace_src");
    charReplaceDest = props.getString("char_replace_dest");
    trim = props.getString("trim_space");
    destPath = props.getString("dest_path");
    destName = props.getString("dest_name");
    //源文件路径和目标文件路径不能为空
    if (StringUtils.isBlank(contentPath) || StringUtils.isBlank(destPath)) {
      throw new RuntimeException("The source path or the dest path can not be empty!");
    }
    //源文件名称为空，默认匹配*
    if (StringUtils.isBlank(contentName)) {
      contentName = "*";
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
    debug("ContentConversionRunnablecomponent ：run");
    try {
      //判断输入框中是否有特殊字符
      detectCharacter();
      //输入源为local
      if ("0".equals(contentSource)) {
        contentConvFileFromLocal();
      } else {
        contentConvFileFromHDFS();
      }
    } catch (Exception e) {
      error("ContentConversion execute failed.", e);
      throw new RuntimeException(e);
    }
  }

  private void detectCharacter() {
    //如果有源字符与替换字符
    if (StringUtils.isNotBlank(charReplaceSrc) && StringUtils.isNotBlank(charReplaceDest)) {
      //判断输入替换字符是否为特殊字符
      if (regExMat(charReplaceSrc)) {
        charReplaceSrc = regExAdd(charReplaceSrc);
        debug("The source special characters: " + charReplaceSrc);
      }
      if (regExMat(charReplaceDest)) {
        charReplaceDest = regExAdd(charReplaceDest);
        debug("The dest special characters: " + charReplaceDest);
      }
    }
  }

  //输入源为local
  public void contentConvFileFromLocal() {
    try {
      File file = new File(contentPath);
      //判断该路径是否合法
      int flag = 0;
      if (!file.exists()) {
        throw new RuntimeException("The file path does not exist!");
      }
      if (!file.isDirectory()) {
        debug("This is not a folder.");
        throw new RuntimeException("The source file path must be a folder!");
      } else if (file.isDirectory()) {
        debug("This is a folder.");
        //查询通配符文件名
        MyFileFilter mm = new MyFileFilter(contentPath, contentName);
        String[] m_filerName = mm.fileFilterFun();
        if (null == m_filerName || m_filerName.length <= 0) {
          throw new RuntimeException("The source file is null！");
        }
        for (int i = 0; i < m_filerName.length; i++) {
          File readfile = new File(contentPath + File.separator + m_filerName[i]);
          String absolutepath = readfile.getAbsolutePath();//文件的绝对路径
          String filename = readfile.getName();//读到的文件名
          File f = new File(destPath, filename);
          String out_filename = destPath + File.separator + filename;
          //////// 开始挨个的读取文件  ////////
          FileStream fsm = new FileStream();
          this.contentConver(fsm.createInStream(absolutepath, contentSource),
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

  //输入源为HDFS
  public void contentConvFileFromHDFS() {
    try {
      //fs文件系统
      FileSystem fs = FileSystem.get(ConfigurationClient.getInstance().getConfiguration());
      Path dir = new Path(contentPath);
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
        FileStatus[] status = fs.globStatus(new Path(contentPath, contentName));
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
          this.contentConver(fsm.createInStream(inputHDFSpath, contentSource),
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

  //内容转换实现
  public void fun(BufferedReader bufReader, BufferedWriter bufWriter) {
    try {
      for (String temp = null; (temp = bufReader.readLine()) != null; temp = null) {
        //去掉首尾空格
        temp = temp.trim();
        //去掉首尾特殊字符
        temp = trimFirstAndLastChar(temp, trim);
        String result = null;
        String regEx = charReplaceSrc;
        Pattern p = Pattern.compile(regEx);
        Matcher m = p.matcher(temp);
        result = m.replaceAll(charReplaceDest);
        bufWriter.write(result + "\n");
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * 去除字符串首尾出现的某个字符.
   *
   * @param source  源字符串.
   * @param element 需要去除的字符.
   * @return String.
   */
  public String trimFirstAndLastChar(String source, String element) {
    String result = "";
    boolean flag = false;
    try {
      if ((source.indexOf(element) == 0)) {
        result = source.substring(element.length(), source.length());
        flag = true;
      }
      //如果首没有，尾有的时候需要赋原始数据行
      if (flag == false) {
        result = source;
      }
      if (result.lastIndexOf(element) != -1) {
        if (result.lastIndexOf(element) == (result.length() - element.length())) {
          result = result.substring(0, result.length() - element.length());
          flag = true;
        }
      }
      if (flag == false) {
        result = source;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  //判断输入的字符串是否为特殊字符，特殊字符前添加“\”
  public String regExAdd(String str) {
    String subStr = "";
    String result = "";
    for (int i = 0; i < str.length(); i++) {
      subStr = str.substring(i, i + 1);
      if (regExMat(subStr)) {
        subStr = "\\" + subStr;
        result += subStr;
      } else {
        result += subStr;
      }
    }
    debug("Convert the special string to：" + result);
    return result;
  }

  //判断输入替换字符是否为特殊字符
  public boolean regExMat(String srcString) {
    try {
      boolean b = false;
      String regExMatch = "[\\s~·`!！@#￥$%^……&*+{}\\[\\]（()）\\-——\\|、\\\\；;：:‘'“”\"，,《<。.》>、/？?]";
      Pattern p1 = Pattern.compile(regExMatch); // 正则表达式
      Matcher mm = p1.matcher(srcString); // 操作的字符串
      b = mm.find(); //返回是否匹配的结果
      return b;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void contentConver(InputStream in, OutputStream out) throws IOException {

    BufferedReader bufReader = new BufferedReader(new InputStreamReader(in, defaultCharSet));
    BufferedWriter bufWriter = new BufferedWriter(new OutputStreamWriter(out));

    //内容转换实现
    fun(bufReader, bufWriter);
    bufReader.close();
    bufWriter.close();
  }
}
