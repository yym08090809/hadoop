import org.junit.Test;

import java.io.*;

public class SplitTest {
    String inputPath = "E:\\Program Files\\JetBrains\\JAVA_Project\\hadoop\\src\\main\\resources\\Phone.txt";

    @Test
    public void Split() throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(inputPath), "utf-8"));
        String s = null;
        s = br.readLine();
        //System.out.println(s);
        //while ((s = br.readLine()) != null){
            String[] s1 = s.split("\t");
        for (int i = 0; i < 4; i++) {
            s1[0] = String.valueOf(Integer.valueOf(s1[0])/10000);
            System.out.println(s1[i]);
        }
       // }
    }
}
