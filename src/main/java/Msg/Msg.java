package Msg;
import java.io.Serializable;
import java.util.Date;

public class Msg implements Serializable{
    private static String id;
    private static Date date;
    public Msg() {
        this.id = IDGenerator.create(20);
        this.date = new Date(System.currentTimeMillis());
    }
    public static String getId() {
        return id;
    }
    public static Date getDate() {
        return date;
    }
}