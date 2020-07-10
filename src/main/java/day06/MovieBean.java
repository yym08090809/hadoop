package day06;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MovieBean implements Writable, DBWritable {
    private String movie;
    private int rate;
    private String ts;
    private int uid;

    public MovieBean() {

    }

    public MovieBean(String movie, Integer rate, String ts, Integer uid) {
        this.movie = movie;
        this.rate = rate;
        this.ts = ts;
        this.uid = uid;
    }

    public String getMovie() {
        return movie;
    }

    public void setMovie(String movie) {
        this.movie = movie;
    }

    public Integer getRate() {
        return rate;
    }

    public void setRate(Integer rate) {
        this.rate = rate;
    }

    public String getTimeStamp() {
        return ts;
    }

    public void setTimeStamp(String ts) {
        this.ts = ts;
    }

    public Integer getUid() {
        return uid;
    }

    public void setUid(Integer uid) {
        this.uid = uid;
    }

    public void set(MovieBean movieBean) {
        movie = movieBean.movie;
        rate = movieBean.rate;
        ts = movieBean.ts;
        uid = movieBean.uid;
    }

    @Override
    public String toString() {
        return "MovieBean{" +
                "movie='" + movie + '\'' +
                ", rate=" + rate +
                ", ts='" + ts + '\'' +
                ", uid=" + uid +
                '}';
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(movie);
        dataOutput.writeInt(rate);
        dataOutput.writeUTF(ts);
        dataOutput.writeInt(uid);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        movie = dataInput.readUTF();
        rate = dataInput.readInt();
        ts = dataInput.readUTF();
        uid = dataInput.readInt();
    }

    @Override
    public void write(PreparedStatement ps) throws SQLException {
        ps.setString(1,movie);
        ps.setInt(2,rate);
        ps.setString(3,ts);
        ps.setInt(4,uid);
    }

    @Override
    public void readFields(ResultSet rs) throws SQLException {
        movie = rs.getString(movie);
        rs.getInt(rate);
        rs.getString(ts);
        rs.getInt(uid);
    }
}
