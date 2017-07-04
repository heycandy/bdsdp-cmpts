package com.chinasofti.ark.bdadp.component;

import com.chinasofti.ark.bdadp.component.api.Configureable;
import com.chinasofti.ark.bdadp.component.api.Optional;
import com.chinasofti.ark.bdadp.component.api.RunnableComponent;
import com.chinasofti.ark.bdadp.component.api.options.ScenarioOptions;
import com.chinasofti.ark.bdadp.util.common.StringUtils;

import org.slf4j.Logger;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Types;
import java.text.SimpleDateFormat;

/**
 * Created by wumin on 2016/9/23.
 */
public class SQLProcess extends RunnableComponent implements Configureable, Optional {

  private final static String DATE_FORMAT = "yyyy-MM-dd";
  private final static String DATE_FORMAT_SHORT = "yyyyMMdd";
  private final static String
      DATE_FORMAT_REGEX =
      "([0-9]{3}[1-9]|[0-9]{2}[1-9][0-9]{1}|[0-9]{1}[1-9][0-9]{2}|[1-9][0-9]{3})-(((0[13578]|1[02])-(0[1-9]|[12][0-9]|3[01]))|((0[469]|11)-(0[1-9]|[12][0-9]|30))|(02-(0[1-9]|[1][0-9]|2[0-8])))";

  private String driver;
  private String url;
  private String user;
  private String pwd;
  private String sql;
  private String inputStr;
  private String outputStr;
  private String resultCode;

  private String inputDate;

  public SQLProcess(String id, String name, Logger log) {
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

    if (StringUtils.isNulOrEmpty(driver)) {
      throw new RuntimeException("Jdbc driver is required.");
    }

    if (StringUtils.isNulOrEmpty(url)) {
      throw new RuntimeException("Jdbc url is required.");
    }
  }

  @Override
  public void options(ScenarioOptions scenarioOptions) {
    inputDate = scenarioOptions.getSettings().get("scenario.assert.date");
    info("input date: " + inputDate);
  }

  //  {CALL pro_num_user(?,?)}
  @Override
  public void run() {
    Connection con = null;
    CallableStatement cstm = null;

    try {
      String regexp = "\'";
      int countIn = inputStr.split(",").length;
      int countSum = sql.split(",").length;
      String[] list_In = inputStr.split(",");

      con = getConnection(driver, url, user, pwd);
      cstm = con.prepareCall(sql);

      for (int i = 1; i <= countIn; i++) {
        if (i == 1) {
          java.sql.Date sqlDate;

          if (StringUtils.isNulOrEmpty(inputDate)) { // 场景没有时间入参的，取list_In第二个值为时间入参
            inputDate = list_In[i - 1].replaceAll(regexp, "");
          }

          if (inputDate.matches(DATE_FORMAT_REGEX)) { // yyyy-MM-dd
            SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
            sqlDate = new java.sql.Date(sdf.parse(inputDate).getTime());

          } else {  // yyyyMMdd
            SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_SHORT);
            sqlDate = new java.sql.Date(sdf.parse(inputDate).getTime());

          }

          info("input date is： " + sqlDate);
          cstm.setDate(1, sqlDate);
        } else {  // 有单引号为字符串入参
          if (list_In[i - 1].contains("'")) {
            String strInput = list_In[i - 1].replaceAll(regexp, "");

            cstm.setString(i, strInput);
            info("input string is： " + strInput);
          } else {  // 没有单引号为数值类型入参
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

      for (int j = countIn + 1; j <= countSum && index_type < list_Out.length; j++) {
        if (list_Out[index_type].equals("string")) {
          cstm.registerOutParameter(j, Types.VARCHAR); // 设置返回值string类型
        } else {
          cstm.registerOutParameter(j, Types.INTEGER); // 设置返回值int类型
        }

        index_type++;
      }

      cstm.execute(); // 执行存储过程

      //  存储过程节点执行后的返回值目前定义的有两个，第一个是Code，第二个是Msg
      for (int k = countIn + 1; k <= countSum && index_get < list_Out.length; k++) {
        if (list_Out[index_get].equals("string")) {
          String str = cstm.getString(k);
          if (org.apache.commons.lang.StringUtils.isNumeric(str)) { //  Number类型的是Code
            runCode = str;
            info("output code is： " + runCode);
          } else {  //  字符串类型的是Msg
            resultMessage = str;
            info("output msg is： " + resultMessage);
          }
        } else {  // 数值类型的忽略
          info("output int is： " + cstm.getInt(k));
        }

        index_get++;
      }

      if (!resultCode.equals(runCode)) {
        throw new SQLException(String.format("Code: %s, Msg: %s", runCode, resultMessage));
      }

    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    } finally {
      if (cstm != null) {
        try {
          cstm.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }

      if (con != null) {
        try {
          con.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
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

}
