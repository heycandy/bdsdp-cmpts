package com.chinasofti.ark.bdadp.component;

import com.chinasofti.ark.bdadp.component.api.Configureable;
import com.chinasofti.ark.bdadp.component.api.RunnableComponent;
import com.chinasofti.ark.bdadp.util.hdfs.common.ConfigurationClient;
import com.chinasofti.ark.bdadp.util.io.MyFileFilter;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Administrator on 2016/9/21.
 */
public class DataMonitor extends RunnableComponent implements Configureable {

  String monitorSource = "";
  String monitorFilePath = "";
  String monitorFileName = "";
  int monitorRetryNumber;
  String monitorRetryPeriod = "";

  String canSkip;

  public DataMonitor(String id, String name, Logger log) {
    super(id, name, log);
  }

  /**
   * 判断 String 是否int
   */
  public static boolean isInteger(String input) {
    Matcher mer = Pattern.compile("^[1-9]\\d*$").matcher(input);
    return mer.find();
  }

  @Override
  public void configure(ComponentProps props) {

    monitorSource = props.getString("file_source");
    monitorFilePath = props.getString("file_path");
    monitorFileName = props.getString("file_name");
    monitorRetryPeriod = props.getString("retry_period");
    monitorRetryNumber = props.getInt("retry_number", 1);

    canSkip = props.getString("can_skip", "no");

    if (StringUtils.isEmpty(monitorFilePath)) {
      throw new RuntimeException(String.format("param %s value is empty.", "file_path"));
    }

    if (StringUtils.isEmpty(monitorFileName)) {
      monitorFileName = "*";
    }

    if (StringUtils.isEmpty(monitorRetryPeriod) || !isInteger(monitorRetryPeriod)) {
      throw new RuntimeException(
          String.format("param %s value is empty or is not integer.", "retry_period"));
    }

  }

  @Override
  public void run() {
    try {
      debug("DataMonitor ：run");

      if ("0".equals(monitorSource)) {
        findLocalFiles();
      } else {
        findHdfsFiles();
      }

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * 查找本地文件
   */
  public void findLocalFiles() {
    try {
      MyFileFilter myFile = new MyFileFilter(monitorFilePath, monitorFileName);
      int i = 0;
      while (i < monitorRetryNumber) {
        i++;

        debug(String.format("monitor local file times %s.", i));
        String[] fileArr = myFile.fileFilterFun();

        if (fileArr != null && fileArr.length > 0) {
          debug(String.format("local file matched: %s", fileArr));
          break;
        } else if (i == monitorRetryNumber) {
          if (canSkip.equals("no")) {
            throw new RuntimeException(
                "monitor has already been retry the maximum number of times for this execute.");
          }
        } else {
          int seconds = Integer.parseInt(monitorRetryPeriod) * 60;
          debug(String.format("retry after %s seconds...", seconds));
          Thread.sleep(seconds * 1000);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * 查找hdfs文件
   */
  public void findHdfsFiles() {
    try {
      FileSystem fs = FileSystem.get(ConfigurationClient.getInstance().getConfiguration());
      int i = 0;
      while (i < monitorRetryNumber) {
        i++;
        debug(String.format("monitor HDFS file times %s.", i));
        FileStatus[] status = fs.globStatus(new Path(monitorFilePath, monitorFileName));
        if (status != null && status.length > 0) {
          debug(String.format("HDFS file matched: %s", status));
          break;
        } else if (i == monitorRetryNumber) {
          if (canSkip.equals("no")) {
            throw new RuntimeException(
                "monitor has already been retry the maximum number of times for this execute.");
          }
        } else {
          int seconds = Integer.parseInt(monitorRetryPeriod) * 60;
          debug(String.format("retry after %s seconds...", seconds));
          Thread.sleep(seconds * 1000);
        }
      }
//      fs.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
