package com.chinasofti.ark.bdadp.component.sample;

import com.chinasofti.ark.bdadp.component.ComponentProps;
import com.chinasofti.ark.bdadp.component.api.Configureable;
import com.chinasofti.ark.bdadp.component.api.RunnableComponent;
import org.slf4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;

/**
 * Created by tangcb on 2016/9/21.
 */
public class ShellComponent extends RunnableComponent implements Configureable {

    /**
     * shell文件所在路径
     **/
    private String shellScriptFilePath;
    /**
     * 执行的shell脚本文件名
     **/
    private String startExecuteShellScript;
    /**
     * shell脚本参数
     **/
    private String shellScriptParam;

    public ShellComponent(String id, String name, Logger log) {
        super(id, name, log);
    }

    @Override
    public void configure(ComponentProps props) {
        this.shellScriptFilePath = props.getString("component_shell_path");
        this.startExecuteShellScript = props.getString("component_shell_filename");
        this.shellScriptParam = props.getString("component_shell_var");
        if (shellScriptParam.contains("illegal")) {
            throw new RuntimeException("The input parameters is illegal: path=" + shellScriptParam);
        }
    }

    @Override
    public void run() {
        String shellScriptFile = shellScriptFilePath + File.separator + startExecuteShellScript;
        // 给shell脚本赋执行权限
        String chmondCommand = "chmod u+x " + shellScriptFile;
        executeScript(chmondCommand, null);
        // 执行shell脚本

        executeScript(shellScriptFile, shellScriptParam);
    }

    /**
     * 执行shell脚本
     */
    private void executeScript(String shellScript, String params) {
        try {
            String commond = shellScript + " " + (params == null ? "" : params);
            Process process = Runtime.getRuntime().exec(new String[]{"/bin/sh", "-c", commond});
            String errorLog = getExecuteLog(process);
            int exitValue = process.waitFor();

            if (exitValue != 0) {
                getLog().error("Run script {} failed. reason: {}", startExecuteShellScript, errorLog);
                throw new RuntimeException();
            }
        } catch (Exception e) {
            getLog().error(e.getMessage());
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    /**
     * 打印执行异常日志
     */
    private String getExecuteLog(Process process) {
        try {
            StringBuffer log = new StringBuffer();
            BufferedReader input = new BufferedReader(new InputStreamReader(process.getErrorStream()));
            String line = "";
            while ((line = input.readLine()) != null) {
                log.append(line);
            }
            input.close();

            return log.toString();
        } catch (Exception e) {
            getLog().error(e.getMessage());
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
