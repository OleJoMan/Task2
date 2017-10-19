package Msg;
import java.util.Random;

public class IDGenerator {
    public static String create(int length) {
        String valid = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
        String res = "";
        Random rnd = new Random();
        while (0 < length--)
            res += valid.charAt(rnd.nextInt(valid.length()));
        return res;
    }
}
