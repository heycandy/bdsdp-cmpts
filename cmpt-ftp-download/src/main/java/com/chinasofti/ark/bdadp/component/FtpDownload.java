package com.chinasofti.ark.bdadp.component;

import com.chinasofti.ark.bdadp.component.api.Configureable;
import com.chinasofti.ark.bdadp.component.api.RunnableComponent;
import com.chinasofti.ark.bdadp.util.hdfs.common.ConfigurationClient;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;

import sun.net.ftp.FtpClient;
import sun.net.ftp.FtpDirEntry;
import sun.net.ftp.FtpProtocolException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

/**
 * Created by Administrator on 2016/9/6.
 */
public class FtpDownload extends RunnableComponent implements Configureable {

  public static int BUFFER_SIZE = 2048;
  //标志位判断是否有可执行源文件
  int flag = 0;
  private String hostName;
  private Integer port;
  private String username;
  private String password;
  private String uploadPath;
  private String fileName;
  private String readDone;
  private String destSource;
  private String destPath;
  private String protocolType;
  /**
   * Sftp客户端对象
   */
  private ChannelSftp sftp;
  private Session sshSession;
  private Channel channel;

  public FtpDownload(String id, String name, Logger log) {
    super(id, name, log);
  }

  @Override
  public void configure(ComponentProps props) {
    hostName = props.getString("host_name");
    port = props.getInt("port");
    username = props.getString("username");
    password = props.getString("password");
    uploadPath = props.getString("upload_path");
    fileName = props.getString("file_name");
    readDone = props.getString("read_done");
    destSource = props.getString("dest_source");
    destPath = props.getString("dest_path");
    protocolType = props.getString("protocol_type");
    if (StringUtils.isEmpty(hostName) || StringUtils.isEmpty(String.valueOf(port))
        || StringUtils.isEmpty(username) || StringUtils.isEmpty(password) || StringUtils
            .isEmpty(uploadPath) || StringUtils.isEmpty(destPath)) {
      throw new RuntimeException("Parameters can not be empty!");
    }
    if (StringUtils.isEmpty(fileName)) {
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
      if ("0".equals(protocolType)) {
        ftp();
      } else {
        sftp();
      }
    } catch (Exception e) {
      error("Ftp/Sftp downloads execute failed.", e);
      throw new RuntimeException(e);
    }
  }

  private void ftp() throws IOException, FtpProtocolException {
    FtpClient client = FtpClient.create();
    SocketAddress address = new InetSocketAddress(hostName, port);

    info(String.format("client connect %s.", address));
    client.connect(address);
    info(String.format("client login %s.", username));
    client.login(username, password.toCharArray());
    client.setBinaryType();

    doTransfer(client);

    info("client close.");
    client.close();
  }

  private void sftp() throws Exception {
    sftp = connect(hostName, port, username, password);
    // 获得SFTP上文件名称列表
    listFile(uploadPath, destPath);
    disConn();
  }

  private void listFile(String uploadPath, String destPath) {
    String file_Name;
    String dest_Path;
    LsEntry isEntity;
    try {
      Vector<LsEntry> sftpFile = sftp.ls(uploadPath);
      Iterator<LsEntry> sftpFileNames = sftpFile.iterator();
      List<String> sftpFileNameList = new ArrayList<String>();

      while (sftpFileNames.hasNext()) {
        isEntity = sftpFileNames.next();
        if (isEntity.getAttrs().isDir()) {
          file_Name = isEntity.getFilename();
          if (".".equals(file_Name) || "..".equals(file_Name)) {
            continue;
          }
          String tmp_path = uploadPath + "/" + isEntity.getFilename();
          String add_outPath = destPath + File.separator + isEntity.getFilename();
          mkDir(add_outPath);
          listFile(tmp_path, add_outPath);
        } else {
          file_Name = isEntity.getFilename();
          if (".".equals(file_Name) || "..".equals(file_Name)) {
            continue;
          }
          sftpFileNameList.add(file_Name);
          // 根据每个FTP文件名称创建本地文件。
          dest_Path = destPath + File.separator + file_Name;
          if (fileName != "*") {
            if (fileName.equals(file_Name)) {
              if ("0".equals(destSource)) {
                download(uploadPath, fileName, dest_Path);
                if ("1".equals(readDone)) {
                  delete(uploadPath, file_Name);
                }
                flag = 1;
                break;
              } else {
                saveToHDFS(uploadPath, fileName, dest_Path);
                if ("1".equals(readDone)) {
                  delete(uploadPath, file_Name);
                }
                flag = 1;
                break;
              }
            } else {
              continue;
            }
          } else {
            if ("0".equals(destSource)) {
              download(uploadPath, file_Name, dest_Path);
              flag = 1;
            } else {
              saveToHDFS(uploadPath, file_Name, dest_Path);
              flag = 1;
            }
            //下载完是否删除源文件
            if ("1".equals(readDone)) {
              delete(uploadPath, file_Name);
            }
          }

        }
      }
      if (0 == flag) {
        throw new RuntimeException(
            "There is no matching file for SFTP downloads in this file path!");
      }
      info("download file list is:" + sftpFileNameList);
    } catch (Exception e) {
      error("Sftp downloads failed.", e);
      throw new RuntimeException(e);
    }
  }

  public void saveToHDFS(String tmp_filePath, String tmp_fileName, String tmp_hdfsPath) {
    try {
      //fs文件系统
      FileSystem fs = FileSystem.get(ConfigurationClient.getInstance().getConfiguration());
      if (!(".".equals(tmp_fileName) || "..".equals(tmp_fileName))) {

        String inputFilePath = tmp_filePath + "/" + tmp_fileName;
        InputStream inputStream = sftp.get(inputFilePath);
        OutputStream outputStream = fs.create(new Path(tmp_hdfsPath));
        IOUtils.copyBytes(inputStream, outputStream, BUFFER_SIZE, false);
        if (inputStream != null) {
          inputStream.close();
          outputStream.close();
        }
      }
    } catch (Exception e) {
      error("Sftp load to HDFS failed.", e);
      throw new RuntimeException(e);
    }
  }

  private void mkDir(String Path) {
    File tempDir = new File(Path);
    if (!tempDir.exists()) {
      tempDir.mkdirs();
    }
  }

  /**
   * 删除文件
   *
   * @param directory  要删除文件所在目录
   * @param deleteFile 要删除的文件
   */
  private void delete(String directory, String deleteFile) throws Exception {
    sftp.cd(directory);
    sftp.rm(deleteFile);
  }


  /**
   * @AddBy: Ares
   * @Description: TODO(connect the host)
   */
  public ChannelSftp connect(String host, int port, String username, String password) {
    ChannelSftp csftp = null;
    JSch jsch = new JSch();
    try {
      sshSession = jsch.getSession(username, host, port);
      info("jsch session created, user=" + username);

      sshSession.setPassword(password);
      Properties sshConfig = new Properties();
      sshConfig.put("StrictHostKeyChecking", "no");
      sshSession.setConfig(sshConfig);
      sshSession.connect();
      info("session is connected.");

      channel = sshSession.openChannel("sftp");
      channel.connect();

      csftp = (ChannelSftp) channel;
      info("connected to host:" + host);

    } catch (JSchException e) {
      error("Sftp connect failed.", e);
      throw new RuntimeException(e);
    }
    return csftp;
  }

  /**
   * @param saveFile //     * @param sftp
   * @AddBy: Ares
   * @Description: TODO(download file from host)
   */
  public boolean download(String directory, String downloadFile, String saveFile) {
    File file = new File(saveFile);
    try {
      sftp.cd(directory);
      sftp.get(downloadFile, new FileOutputStream(file));
      info("download sftp file success, file:" + downloadFile);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
      return false;
    } catch (SftpException e) {
      error("download sftp file failed, file:" + downloadFile);
      e.printStackTrace();
      return false;
    }
    return true;
  }

  public void disConn() throws Exception {
    if (null != sftp) {
      sftp.disconnect();
      sftp.exit();
      sftp = null;
    }
    if (null != channel) {
      channel.disconnect();
      channel = null;
    }
    if (null != sshSession) {
      sshSession.disconnect();
      sshSession = null;
    }
  }

  private void doGetFile(String name, FtpClient client) throws IOException, FtpProtocolException {
    try {
      File file = new File(destPath, name);
      if (file.exists()) {
        info(String.format("dest %s exists, delete it.(%s)", file.getPath(), file.delete()));
      } else {
        info(String.format("dest %s not exists, create new file.(%s)", file.getPath(),
                           file.createNewFile()));
      }

      OutputStream output = new FileOutputStream(file);
      client.getFile(name, output);

      output.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void doTransfer(FtpClient client) throws IOException, FtpProtocolException {

    try {
      client.changeDirectory(uploadPath);
      Iterator<FtpDirEntry> iterator = client.listFiles(fileName);
      if (!iterator.hasNext()) {
//                throw new RuntimeException(fileName + " not exists");
        throw new RuntimeException(
            "There is no matching file for FTP downloads in this file path!");
      }
      while (iterator.hasNext()) {
        FtpDirEntry entry = iterator.next();
        debug(entry.toString());

        if ("0".equals(destSource)) {
          doGetFile(entry.getName(), client);
          flag = 1;
        } else {
          loadFromFtpToHdfs(entry.getName(), client);
          flag = 1;
        }

        if (readDone.equals("1")) {
          client.deleteFile(entry.getName());
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }

  private void loadFromFtpToHdfs(String fileName, FtpClient client) {
    try {
      //fs文件系统
      FileSystem fs = FileSystem.get(ConfigurationClient.getInstance().getConfiguration());
      if (!(fileName.equals(".") || fileName.equals(".."))) {
        InputStream inputStream = client.getFileStream(uploadPath + "/" + fileName);
        OutputStream outputStream = fs.create(new Path(destPath + File.separator + fileName));
        IOUtils.copyBytes(inputStream, outputStream, BUFFER_SIZE, false);
        if (inputStream != null) {
          inputStream.close();
          outputStream.close();
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("loadFromFtpToHdfs execute failed!");
    }
  }
}
