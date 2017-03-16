/**
* @Author : water  
* @Date   : 2017年2月28日 
* @Desc   : TODO
* @version: V1.0
*/
import java.sql.CallableStatement; 
import java.sql.Connection; 
import java.sql.DriverManager; 
import java.sql.Types; 



/* 
 * 对应的存储过程 
 DROP PROCEDURE IF EXISTS `pro_num_user`; 
delimiter ;; 
CREATE PROCEDURE `pro_num_user`(IN user_name varchar(10) ,OUT count_num INT) 
    READS SQL DATA 
BEGIN 
    SELECT COUNT(*) INTO count_num FROM tab_user WHERE 'name'=user_name; 
END 
 ;; 
delimiter ; 
 */ 
public class ttttt { 

    public static final String DRIVER_CLASS = "com.mysql.jdbc.Driver"; 
    public static final String URL = "jdbc:mysql://10.100.66.118:3306/testjdbc";
    public static final String USERNAME = "root"; 
    public static final String PASSWORD = "rootadmin";
    
    public static void main(String[] args) throws Exception { 
        test1(); 
//        test2(); 
    } 
     
    public static void test1() throws Exception 
    { 
        Class.forName(DRIVER_CLASS); 
        Connection connection = DriverManager.getConnection(URL, USERNAME, PASSWORD); 
        String sql = "{CALL pro_num_user(?,?)}"; //调用存储过程
        CallableStatement cstm = connection.prepareCall(sql); //实例化对象cstm 
//        cstm.setString(1, "www"); //存储过程输入参数 
        cstm.setString(1, "hao"); // 存储过程输入参数
        cstm.registerOutParameter(2, Types.INTEGER); // 设置返回值类型 即返回值 
        cstm.execute(); // 执行存储过程 
        System.out.println(cstm.getInt(2)); 
        cstm.close(); 
        connection.close(); 
    } 
     
    public static void test2() throws Exception 
    { 
        Class.forName(DRIVER_CLASS); 
        Connection connection = DriverManager.getConnection(URL, USERNAME, PASSWORD); 
        String sql = "{CALL pro_number(?,?,?)}"; //调用存储过程 
        CallableStatement cstm = connection.prepareCall(sql); //实例化对象cstm 
        cstm.setInt(1, 2); // 存储过程输入参数 
        cstm.setInt(2, 2); // 存储过程输入参数 
        cstm.registerOutParameter(3, Types.INTEGER); // 设置返回值类型 即返回值 
        cstm.execute(); // 执行存储过程 
        System.out.println(cstm.getInt(3)); 
        cstm.close(); 
        connection.close(); 
         
    } 
}