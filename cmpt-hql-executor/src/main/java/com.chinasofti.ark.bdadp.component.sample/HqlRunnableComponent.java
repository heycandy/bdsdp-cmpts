/**
 * Copyright (c) 2016 chinaSofti.com. All Rights Reserved.
 */
package com.chinasofti.ark.bdadp.component.sample;

import com.chinasofti.ark.bdadp.component.ComponentProps;
import com.chinasofti.ark.bdadp.component.api.Configureable;
import com.chinasofti.ark.bdadp.component.api.RunnableComponent;
import com.chinasofti.ark.bdadp.expression.support.ArkConversionUtil;
import com.chinasofti.ark.bdadp.util.hadoop.HiveJdbcUtil;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ${DESCRIPTION}
 *
 * @author wgzhang
 * @create 2016-09-12 17:00
 */
public class HqlRunnableComponent extends RunnableComponent implements Configureable {

    String hqlHql = "";
    String fileName = "";
    String hqlVar = "";
    String hiveConf = "";
    String hqlType = "";

    String zkQuorum = "";// zkQuorum的"xxx.xxx.xxx.xxx"为集群中Zookeeper所在节点的IP，端口默认是24002
    String userName = "";
    String pwd = "";
    String hostName = "";
    Map<String, String> mapParam = new HashMap<String, String>();
    Connection connection = null;

    public HqlRunnableComponent(String id, String name, Logger log) {
        super(id, name, log);
    }

    @Override
    public void configure(ComponentProps props) {

        hqlHql = props.getString("hql");
        hiveConf = props.getString("hive_conf");
        hqlType = props.getString("hql_type");
        if (StringUtils.isEmpty(hqlHql) || StringUtils.isEmpty(String.valueOf(hiveConf))) {
            throw new RuntimeException("HQL and hive configuration parameters cannot be empty!");
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
            throw new RuntimeException("Hive link parameters cannot be empty!");
        }
    }

    @Override
    public void run() {

        try {
            connection = HiveJdbcUtil.getConnection(zkQuorum, userName, pwd);

            if (null == connection) {
                throw new RuntimeException("Link to the server failed!");
            }
        } catch (Exception e) {
            throw new RuntimeException("HqlRunnableComponent exception: ", e);
        }

        try {
            String flag = "0";
            Map<String, List> result = null;
            if ("0".equals(hqlType)) {
                result = HiveJdbcUtil.execHql(connection, "default", hqlHql, 1000);
            } else {

                String[] pairs = hqlHql.split(";");
                for (int i = 0; i < pairs.length; i++) {
                    File file = new File(pairs[i]);
                    if (file.exists()) {
                        if (file.isDirectory()) {
                            flag = "1";
                            for (int j = 0; j < file.list().length; j++) {
                                File readfile = new File(pairs[i] + File.separator + file.list()[i]);
                                String absolutepath = readfile.getAbsolutePath();// 文件的绝对路径
                                //////// 开始挨个的读取文件 ////////
                                // FileReader:用来读取字符文件的便捷类。
                                FileReader reader = null;
                                StringBuffer strBuffer = new StringBuffer();
                                reader = new FileReader(absolutepath);
                                int temp;
                                while ((temp = reader.read()) != -1) {
                                    strBuffer.append((char) temp);
                                }
                                reader.close();

                                ArkConversionUtil util = new ArkConversionUtil();
                                String parseValue = util.parseVariableByDefined(strBuffer.toString());
                                if (parseValue.contains("illegal")) {
                                    throw new RuntimeException(
                                            "Can not find the file (check whether the wildcard is correct): path="
                                                    + parseValue);
                                }
                                debug("original hql:" + parseValue);
                                result = HiveJdbcUtil.execHql(connection, "default", parseValue, 1000);
                                debug(hqlHql + "execute result is: " + result);
                            }
                        } else {
                            flag = "2";
                            File readfile = new File(hqlHql);
                            String absolutepath = readfile.getAbsolutePath();// 文件的绝对路径
                            //////// 开始挨个的读取文件 ////////
                            // FileReader:用来读取字符文件的便捷类。
                            FileReader reader = null;
                            StringBuffer strBuffer = new StringBuffer();
                            reader = new FileReader(absolutepath);
                            int temp;
                            while ((temp = reader.read()) != -1) {
                                strBuffer.append((char) temp);
                            }
                            reader.close();
                            debug("original hql:" + strBuffer.toString());
                            result = HiveJdbcUtil.execHql(connection, "default", strBuffer.toString(), 1000);
                            debug(hqlHql + ": " + result);
                        }
                    } else {
                        throw new RuntimeException("The file path is not correct: path=" + pairs[i]);
                    }
                    if ("0".equals(flag)) {
                        throw new RuntimeException("The file path is not correct: hql=" + hqlHql);
                    }
                }
                if (result == null) {
                    throw new RuntimeException("Can not get the correct result: hql=" + hqlHql);
                }

            }

        } catch (Exception e) {
            throw new RuntimeException("HqlRunnableComponent exception: ", e);
        } finally {
            // 关闭JDBC连接
            HiveJdbcUtil.closeConnection(connection);
        }
    }

}
