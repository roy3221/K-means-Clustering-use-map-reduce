import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
/**
*  自定义字符输入流的包装类，通过这个包装类对底层字符输入流进行包装，
* 让程序通过这个包装类读取某个文本文件（例如，一个java源文件）时，
* 能够在读取的每行前面都加上有行号和冒号。
* @author Administrator
*
*/

public class InputFileInitial {

 BufferedReader reader;
 //该类对象开销比String小
 StringBuilder sb;
 String mapInputPath=null;
 public InputFileInitial(BufferedReader reader) {
         this.reader=reader;
         pack();
 }
 //通过文件路径构造
 public InputFileInitial(String path, String mapInputPath) throws Exception{
         this(new BufferedReader(new FileReader(path)));
         this.mapInputPath=mapInputPath;
 }
 //通过文件对象构造
 public InputFileInitial(File file) throws Exception{
         this(new BufferedReader(new FileReader(file)));
 }
 //对读入的每一行进行加工
 private void pack() {
         try {
                 sb=new StringBuilder();
                 String line=null;
                 int linenum=0;
                 while ((line=reader.readLine())!=null) {
                         sb.append(++linenum+"\t"+line+"\n");
                 }
                 PrintWriter printwriter=new PrintWriter(new FileWriter(mapInputPath,true));
                 printwriter.println(sb); 
                 printwriter.close();
                 
         } catch (Exception e) {
                 // TODO Auto-generated catch block
                 e.printStackTrace();
         }
 }
 //打印加工后的字符串
 public void print() {
         System.out.println(sb.toString());
 }
 
	

}
