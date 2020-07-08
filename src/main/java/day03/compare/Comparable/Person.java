package day03.compare.Comparable;

public class Person implements Comparable {
    private String name;
    private int age;
    private double source;

    public Person() {
    }

    public Person(String name, int age, double source) {
        this.name = name;
        this.age = age;
        this.source = source;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public double getSource() {
        return source;
    }

    public void setSource(double source) {
        this.source = source;
    }

    @Override
    public String toString() {
        return "Person{" +
                "name='" + name + '\'' +
                ", age=" + age +
                ", source=" + source +
                '}';
    }

    @Override
    public int compareTo(Object that) {
        return 0;
    }
}
