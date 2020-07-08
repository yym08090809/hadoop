package day04.Json;

public class MovieBean {
    private String movie;
    private Integer rate;
    private String timeStamp;
    private Integer uid;

    public MovieBean() {

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

    @Override
    public String toString() {
        return "MovieBean{" +
                "movie='" + movie + '\'' +
                ", rate=" + rate +
                ", timeStamp='" + timeStamp + '\'' +
                ", uid=" + uid +
                '}';
    }
}
