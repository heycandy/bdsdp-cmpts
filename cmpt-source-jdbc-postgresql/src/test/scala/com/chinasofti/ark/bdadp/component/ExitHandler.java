//package com.chinasofti.ark.bdadp.component;
//
//import java.sql.SQLException;
//import java.sql.Statement;
//
///**
// * Created by water on 2017.7.27.
// */
//public class ExitHandler extends Thread {
//  private Statement cancel_stmt = null;
//
//  public ExitHandler(Statement stmt) {
//    super("Exit Handler");
//    this.cancel_stmt = stmt;
//  }
//  public void run() {
//    System.out.println("exit handle");
//    try {
//      this.cancel_stmt.cancel();
//    } catch (SQLException e) {
//      System.out.println("cancel queyr failed.");
//      e.printStackTrace();
//    }
//  }
//}
