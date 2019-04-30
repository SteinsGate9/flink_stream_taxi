import java.io.*;
import java.nio.charset.Charset;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

public class java {
    public static void main(String[] args) throws IOException {
//        String pathname = "hdfs://hadoop1:9000/user/root/logs.txt";
//        try (FileReader reader = new FileReader(pathname);
//             BufferedReader br = new BufferedReader(reader) // 建立一个对象，它把文件内容转成计算机能读懂的语言
//        ) {
//            String line;
//            //网友推荐更加简洁的写法
//            while ((line = br.readLine()) != null) {
//                // 一次读入一行数据
//                System.out.println(line);
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

//        String path = "hdfs://hadoop1:9000/user/root/logs.zip";
//        ZipFile zf = new ZipFile(path);
//        ZipInputStream zin = new ZipInputStream(new BufferedInputStream(new FileInputStream(path)),Charset.forName("gbk"));
//        ZipEntry ze = zin.getNextEntry();
//        BufferedReader br = new BufferedReader(new InputStreamReader(zf.getInputStream(ze)));
//        String line;
//        int a = 0;
//        while((line = br.readLine()) != null && a < 100000){
//            System.out.println(line.toString());
//            a += 1;
//        }
//        br.close();
//        zin.closeEntry();



    }
}

