package com.chinasofti.ark.bdadp.component;

import com.chinasofti.ark.bdadp.component.api.Configureable;
import com.chinasofti.ark.bdadp.component.api.RunnableComponent;
import com.chinasofti.ark.bdadp.util.common.StringUtils;
import java.util.Properties;
import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;
import org.springframework.mail.javamail.JavaMailSenderImpl;
import org.slf4j.Logger;
import org.springframework.mail.javamail.MimeMessageHelper;

/**
 * Created by water on 2017.5.9.
 */
public class Mail extends RunnableComponent implements Configureable {

  static String ENCODING = "utf-8";
  String host = "";
  String port = "";
  String userName = "";
  String password = "";
  String receiver = "";

  Properties sendProps = new Properties();

  public Mail(String id, String name, Logger log) {
    super(id, name, log);
  }


  @Override
  public void configure(ComponentProps props) {
    host = props.getString("host");
    port = props.getString("port", "25");
    userName = props.getString("userName");
    password = props.getString("password");
    receiver = props.getString("receiver");

    StringUtils.assertIsBlank(host, port, userName, password, receiver);
  }

  @Override
  public void run() {
    if ("assertFalse".equalsIgnoreCase(System.getProperty("assertKey"))){
      String context = System.getProperty("assertContext");
      String subject = "";
      sendMail(subject,context,receiver);
    }
    else{
      sendMail("日志","错误日志",receiver);
    }

  }

  public JavaMailSenderImpl getSender() {

    JavaMailSenderImpl sender = new JavaMailSenderImpl();
    sender.setHost(host);
    sender.setPort(Integer.parseInt(port));
    sender.setUsername(userName);
    sender.setPassword(password);

    sendProps.setProperty("defaultEncoding", ENCODING);
    sendProps.setProperty("mail.smtp.auth", "false");
    sendProps.setProperty("mail.smtp.starttls.enable", "false");
    sender.setJavaMailProperties(sendProps);
    return sender;
  }


  public void sendMail(String subject, String context, String receiver) {
    sendMail(subject, context, receiver, getSender());
  }


  public void sendMail(String subject, String context, String receiver, JavaMailSenderImpl sender) {
    MimeMessage mimeMessage = sender.createMimeMessage();
    MimeMessageHelper message;
    try {
      message = new MimeMessageHelper(mimeMessage, false, ENCODING);
      message.setTo(receiver);
      message.setFrom(sender.getUsername());
      message.setSubject(subject);
      message.setText(context);
      sender.send(mimeMessage);
      debug(" send mail success!!!");

    } catch (MessagingException e) {
      e.printStackTrace();
    }
  }


}

