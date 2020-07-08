package day04.Alltop;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MovieBean implements Writable {
    private String movie;
    private Integer rate;
    private String timeStamp;
    private Integer uid;

    public MovieBean() {

    }

    public MovieBean(String movie, Integer rate, String timeStamp, Integer uid) {
        this.movie = movie;
        this.rate = rate;
        this.timeStamp = timeStamp;
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
        return timeStamp;
    }

    public void setTimeStamp(String timeStamp) {
        this.timeStamp = timeStamp;
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
        timeStamp = movieBean.timeStamp;
        uid = movieBean.uid;
    }

    @Override
    public String toString() {
        return "MovieBean{" +
                "movie='" + movie + '\'' +
                ", rate=" + rate +
                ", timeStamp='" + timeStamp + '\'' +
                ", uid=" + uid +
                '}';
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(movie);
        dataOutput.writeInt(rate);
        dataOutput.writeUTF(timeStamp);
        dataOutput.writeInt(uid);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        movie = dataInput.readUTF();
        rate = dataInput.readInt();
        timeStamp = dataInput.readUTF();
        uid = dataInput.readInt();
    }
}
