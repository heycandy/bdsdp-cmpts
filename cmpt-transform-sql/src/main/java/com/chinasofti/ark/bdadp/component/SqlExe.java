package com.chinasofti.ark.bdadp.component;

import com.chinasofti.ark.bdadp.component.api.Configureable;
import com.chinasofti.ark.bdadp.component.api.RunnableComponent;
import com.chinasofti.ark.bdadp.util.common.StringUtils;
<<<<<<< HEAD
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import org.slf4j.Logger;
import java.sql.*;

import java.sql.Connection;
import java.sql.DriverManager;
=======
import org.slf4j.Logger;

import java.sql.*;
>>>>>>> c5c6e652a6967989a1d0e5a8aa802015dea6fab4

/**
 * Created by wumin on 2016/9/23.
 */
public class SqlExe extends RunnableComponent implements Configureable {

<<<<<<< HEAD
  private final static String DEST_CHARSET = "UTF-8";
  private final static String DATE_FORMAT = "yyyy-MM-dd";
  private String driver;
  private String url;
  private String user;
  private String pwd;
  private String sql;
  private String inputStr;
  private String outputStr;
  private String resultCode;


  public SqlExe(String id, String name, Logger log) {
    super(id, name, log);
  }

  @Override
  public void configure(ComponentProps props) {
    driver = props.getString("jdbc_driver");
    url = props.getString("jdbc_url");
    user = props.getString("jdbc_user");
    pwd = props.getString("jdbc_pwd");
    sql = props.getString("jdbc_sql");
    inputStr = props.getString("inputStr");
    outputStr = props.getString("outputStr");
    resultCode = props.getString("resultCode","0").trim();
    StringUtils.assertIsBlank(driver, url, user, pwd, sql, inputStr, outputStr);
    checkParams();
  }

  //      String sql = "{CALL pro_num_user(?,?)}"; //调用存储过程
  @Override
  public void run() {
    Connection con = getConnection(driver, url, user, pwd);
    try {
      String regexp = "\'";
      int countIn = inputStr.split(",").length;
      int countSum = sql.split(",").length;
      String[] list_In = inputStr.split(",");
      CallableStatement cstm = con.prepareCall(sql);
      for (int i = 1; i <= countIn; i++) {

        if (i == 1) {
          SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
          String inputDate = System.getProperty("scenario.assert.date");
          if (org.apache.commons.lang.StringUtils.isBlank(inputDate)) {
            inputDate = list_In[i - 1].replaceAll(regexp, "");
          }
          java.sql.Date sqlDate = new java.sql.Date(sdf.parse(inputDate).getTime());
          info("input date is： " + sqlDate);
          cstm.setDate(1, sqlDate);
        } else {

          if (list_In[i - 1].indexOf("'") > -1) {//有单引号
            String strInput = list_In[i - 1].replaceAll(regexp, "");

            cstm.setString(i, strInput);
            info("input string is： " + strInput);

          } else {
            cstm.setInt(i, Integer.parseInt(list_In[i - 1]));
            info("input int is： " + Integer.parseInt(list_In[i - 1]));
          }
        }
      }

      String[] list_Out = outputStr.split(",");
      int index_type = 0;
      int index_get = 0;
      String resultMessage = null;
      String runCode = null;
      for (int j = countIn + 1; j <= countSum; j++) {
        if (list_Out[index_type].equals("string")) {
          cstm.registerOutParameter(j, Types.VARCHAR); // 设置返回值string类型
          index_type++;
//          info("output type is： " + Types.VARCHAR);
        } else {
          cstm.registerOutParameter(j, Types.INTEGER); // 设置返回值int类型
          index_type++;
//          info("output type is： " + Types.INTEGER);
        }
      }
      cstm.execute(); // 执行存储过程
      for (int k = countIn + 1; k <= countSum; k++) {
        if (list_Out[index_get].equals("string")) {

          if(isNum(cstm.getString(k))){

            runCode = cstm.getString(k);
            info("output code is： " + runCode);
          }else{
            resultMessage = cstm.getString(k);
            info("output msg is： " + resultMessage);
          }
          index_get++;
        } else {
          info("output code is： " + cstm.getInt(k));
          index_get++;
        }
      }
      if (!resultCode.equals(runCode)) {
        throw new RuntimeException(" execute sql failed, message is: " + resultMessage);
      }
      cstm.close();
      con.close();
    } catch (SQLException e) {
      throw new RuntimeException(getName() + " execute sql failed, sql is: ");
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }

  private void checkParams() {
    if (driver == null || "".equals(driver.trim())) {
      throw new RuntimeException("Jdbc driver is required.");
    }
    if (url == null || "".equals(url.trim())) {
      throw new RuntimeException("Jdbc url is required.");
    }
  }

  private Connection getConnection(String driver, String url, String user, String pwd) {
    try {
      Class.forName(driver);
      Connection con = DriverManager.getConnection(url, user, pwd);
      if (con == null) {
        throw new RuntimeException(
            getName() + " can not work since it can not retrieve connection.");
      }
      return con;
    } catch (Exception e) {
      throw new RuntimeException(
          "Can not create jdbc connection with URL '" + url + "', please check details: " + e
              .getMessage());
    }
  }

  private  boolean isNum(String str) {

    try {
      new BigDecimal(str);
      return true;
    } catch (Exception e) {
      return false;
    }
  }


=======
    private final static String DEST_CHARSET = "UTF-8";
    private String driver;
    private String url;
    private String user;
    private String pwd;
    private String sql;
    private String inputStr;
    private String outputStr;


    public SqlExe(String id, String name, Logger log) {
        super(id, name, log);
    }

    @Override
    public void configure(ComponentProps props) {
        driver = props.getString("jdbc_driver");
        url = props.getString("jdbc_url");
        user = props.getString("jdbc_user");
        pwd = props.getString("jdbc_pwd");
        sql = props.getString("jdbc_sql");
        inputStr = props.getString("inputStr");
        outputStr = props.getString("outputStr");
        StringUtils.assertIsBlank(driver, url, user, pwd, sql, inputStr, outputStr);
        checkParams();
    }

    @Override
    public void run() {
        Connection con = getConnection(driver, url, user, pwd);
        if (con == null) {
            throw new RuntimeException(getName() + " can not work since it can not retrieve connection.");
        }
//    ResultSet rs = null;
        try {
//      String sql = "{CALL pro_num_user(?,?)}"; //调用存储过程
            String regexp = "\'";
            int countIn = inputStr.split(",").length;
            int countSum = sql.split(",").length;
            String[] list_In = inputStr.split(",");
            CallableStatement cstm = con.prepareCall(sql); //实例化对象cstm
            for (int i = 1; i <= countIn; i++) {
                if (list_In[i - 1].indexOf("'") > -1) {
                    //有单引号
                    String str_Int = list_In[i - 1].replaceAll(regexp, "");
                    cstm.setString(i, str_Int); //存储过程输入string参数
                    info("input string is： " + str_Int);
                } else {
                    cstm.setInt(i, Integer.parseInt(list_In[i - 1])); //存储过程输入int参数
                    info("input int is： " + Integer.parseInt(list_In[i - 1]));
                }
            }
            //cstm.setInt(2, 2); // 存储过程输入参数
            String[] list_Out = outputStr.split(",");
            int index_type = 0;
            int index_get = 0;
            for (int j = countIn + 1; j <= countSum; j++) {
                if (list_Out[index_type].equals("string")) {
                    cstm.registerOutParameter(j, Types.VARCHAR); // 设置返回值string类型
                    index_type++;
                    info("output type is： " + Types.VARCHAR);
                } else {
                    cstm.registerOutParameter(j, Types.INTEGER); // 设置返回值int类型
                    index_type++;
                    info("output type is： " + Types.INTEGER);
                }
            }
            cstm.execute(); // 执行存储过程
            for (int k = countIn + 1; k <= countSum; k++) {
                if (list_Out[index_get].equals("string")) {
                    info("output is： " + cstm.getString(k));
                    index_get++;
                } else {
                    info("output is： " + cstm.getInt(k));
                    index_get++;
                }
            }
            cstm.close();
            con.close();
        } catch (SQLException e) {
            throw new RuntimeException(getName() + " execute sql failed, sql is: ");
        }
    }

    private void checkParams() {
        if (driver == null || "".equals(driver.trim())) {
            throw new RuntimeException("Jdbc driver is required.");
        }
        if (url == null || "".equals(url.trim())) {
            throw new RuntimeException("Jdbc url is required.");
        }
//    if (sql == null || "".equals(sql.trim())) {
//      throw new RuntimeException("Jdbc sql is required.");
//    }
    }

    private Connection getConnection(String driver, String url, String user, String pwd) {
        try {
            Class.forName(driver);
            return DriverManager.getConnection(url, user, pwd);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Can not create jdbc connection with URL '" + url + "', please check details: " + e
                            .getMessage());
        }
    }
>>>>>>> c5c6e652a6967989a1d0e5a8aa802015dea6fab4
}
