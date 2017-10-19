package Stub;

import java.io.Serializable;

public class Response implements Serializable{
    private static String id;
    private static String status;
    public Response(String id, String status) {
        this.id = id;
        this.status = status;
    }
    public static String getId() {
        return id;
    }
    public static String getStatus() {
        return status;
    }
}