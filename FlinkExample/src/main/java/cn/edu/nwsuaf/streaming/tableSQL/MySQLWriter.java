package cn.edu.nwsuaf.streaming.tableSQL;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @ClassName: MySQLWriter
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/3/12 7:34 下午
 */

public class MySQLWriter extends RichSinkFunction<Info> {
    private Connection connection;
    private PreparedStatement preparedStatement;


    @Override
    public void invoke(Info value, Context context) throws Exception {
        String userId = value.userId;
        String name = value.name;
        String sex = value.sex;
        String age = value.age;
        long createTime = value.createTime;
        long updateTime = value.updateTime;

        preparedStatement.setString(1, userId);
        preparedStatement.setString(2, name);
        preparedStatement.setString(3, age);
        preparedStatement.setString(4, sex);
        preparedStatement.setLong(5, createTime);
        preparedStatement.setLong(6, updateTime);

        int i = preparedStatement.executeUpdate();
        if (i > 0) {
            System.out.println("value=" + value);
        } else {
            System.out.println("error");
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        String driver = "com.mysql.jdbc.Driver";
        Class.forName(driver);
        String url = "jdbc:mysql://localhost:3306/test";
        String user = "root";
        String pass = "123456";
        connection = DriverManager.getConnection(url, user, pass);
        String sql = "REPLACE INTO user(userId,name,age,sex,createTime,updateTime) values(?,?,?,?,?,?)";
        preparedStatement = connection.prepareStatement(sql);

    }

    @Override
    public void close() throws Exception {
        super.close();
        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
}
