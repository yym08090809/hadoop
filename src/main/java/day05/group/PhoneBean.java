package day05.group;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PhoneBean implements Writable {
    private String phoneNumber;
    private Integer upData;
    private Integer downData;

    public PhoneBean() {
    }

    public PhoneBean(String phoneNumber, Integer upData, Integer downData) {
        this.phoneNumber = phoneNumber;
        this.upData = upData;
        this.downData = downData;
    }
    public void set(PhoneBean bean) {
        this.phoneNumber = bean.phoneNumber;
        this.upData = bean.upData;
        this.downData = bean.downData;
    }

    public String getPhoneNumber() {
        return phoneNumber;
    }

    public void setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
    }

    public Integer getUpData() {
        return upData;
    }

    public void setUpData(Integer upData) {
        this.upData = upData;
    }

    public Integer getDownData() {
        return downData;
    }

    public void setDownData(Integer downData) {
        this.downData = downData;
    }

    @Override
    public String toString() {
        return phoneNumber + "::" +
               upData + "::" +
                downData + "::" +
                upData+downData;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(phoneNumber);
        dataOutput.writeInt(upData);
        dataOutput.writeInt(downData);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        phoneNumber = dataInput.readUTF();
        upData = dataInput.readInt();
        downData = dataInput.readInt();
    }
}
