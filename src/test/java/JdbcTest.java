import org.junit.Test;

import java.sql.*;

public class JdbcTest {
    private final String driverClass = "com.mysql.jdbc.Driver";
    private final String dbUrl = "jdbc:mysql://127.0.0.1:3306/test?useUnicode=true&amp;characterEncoding=uft8&amp;serverTimezone=GMT";
    private final String userName = "root";
    private final String passWord = "123456";

    @Test
    public void JdbcTestDemo() throws ClassNotFoundException, SQLException {
        Class.forName(driverClass);
        Connection conn = DriverManager.getConnection(dbUrl, userName, passWord);
        Statement stat = conn.createStatement();
        String sql = "select * from student";
        ResultSet rs = stat.executeQuery(sql);
        while (rs.next()) {
            int id = rs.getInt("id");
            String name = rs.getString(2);
            System.out.println(id+name);
        }

    }
}
