package day05.join;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UserBean implements Writable {
    private String userId;
    private String sex;
    private Integer age;
    private String movieId;
    private Integer rate;

    //辨别表名
    private String tableName;


    public UserBean() {
    }

    public UserBean(String userId, String sex, Integer age, String movieId, Integer rate, String tableName) {
        this.userId = userId;
        this.sex = sex;
        this.age = age;
        this.movieId = movieId;
        this.rate = rate;
        this.tableName = tableName;
    }
    public void set(String userId, String sex, Integer age, String movieId, Integer rate, String tableName) {
        this.userId = userId;
        this.sex = sex;
        this.age = age;
        this.movieId = movieId;
        this.rate = rate;
        this.tableName = tableName;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public String getMovieId() {
        return movieId;
    }

    public void setMovieId(String movieId) {
        this.movieId = movieId;
    }

    public Integer getRate() {
        return rate;
    }

    public void setRate(Integer rate) {
        this.rate = rate;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

//    @Override
//    public String toString() {
//        return "UserBean{" +
//                "userId='" + userId + '\'' +
//                ", sex='" + sex + '\'' +
//                ", age=" + age +
//                ", movieId='" + movieId + '\'' +
//                ", rate=" + rate +
//                ", tableName='" + tableName + '\'' +
//                '}';
//    }
        @Override
    public String toString() {
        return userId + "::" +
                sex + "::" +
                age + "::" +
                movieId + "::" +
                rate ;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(userId);
        dataOutput.writeUTF(sex);
        dataOutput.writeInt(age);
        dataOutput.writeUTF(movieId);
        dataOutput.writeInt(rate);
        dataOutput.writeUTF(tableName);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        userId = dataInput.readUTF();
        sex = dataInput.readUTF();
        age = dataInput.readInt();
        movieId = dataInput.readUTF();
        rate = dataInput.readInt();
        tableName = dataInput.readUTF();
    }
}
