import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.security.PublicKey;
import java.util.*;
import javax.imageio.ImageIO;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.util.Base64;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
class MyThread extends Thread {
    private String cmd;
    public MyThread(String name) {
        this.cmd = name;
    }
    public void run(){

    }
}
public class MyServer1 extends HttpServlet{
    private HashMap<String,HashMap<Integer,Dataset>> m_datasets;
    private HashMap<String,User> m_users;
    private int responsetype=0;
    private List<Dataset> datasets;
    private byte[] resbuf;
    private final String root_path="/root/datapedia/datahub/repositories/";
    public MyServer1(){
        m_datasets=new HashMap<String,HashMap<Integer,Dataset>>();
        datasets=new ArrayList<Dataset>();
        m_users=new HashMap<String,User>();
    }
    public int get_data_type(String name) throws IOException {
        File type_file=new File(root_path+name+"/inf.txt");
        BufferedReader in = new BufferedReader(new FileReader(type_file));
        String str;
        int data_type=0;
        if ((str = in.readLine()) != null) {
            data_type = Integer.parseInt(str);
            System.out.println(Integer.parseInt(str));
        }
        in.close();
        return data_type;
    }

    public boolean isInteger(String value) {
        try {
            Integer.parseInt(value);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public static void compress(File sourceFile, ZipOutputStream zos, String name, int level)throws Exception
    {
        byte[] buf = new byte[2 * 1024];
        if (level == 2)// 找文件
        {
            if (sourceFile.isFile()) {
                // 向zip输出流中添加一个zip实体，构造器中name为zip实体的文件的名字
                zos.putNextEntry(new ZipEntry(name));
                // copy文件到zip输出流中
                int len1;
                FileInputStream in = new FileInputStream(sourceFile);
                while ((len1 = in.read(buf)) != -1) {
                    zos.write(buf, 0, len1);
                }
                // Complete the entry
                zos.closeEntry();
                in.close();
            }
        }
        else //找文件夹
        {
            if (sourceFile.isDirectory() && name.indexOf("tmp") == -1){
                File[] listFiles = sourceFile.listFiles();
                if (listFiles == null || listFiles.length == 0) {
                    zos.putNextEntry(new ZipEntry(name + "/"));
                    zos.closeEntry();

                } else {
                    for (File file : listFiles) {
                        compress(file, zos, name + "/" + file.getName(), level + 1);
                    }
                }
            }
        }
    }


    public static void toZip(String srcDir, OutputStream out)
            throws RuntimeException{

        long start = System.currentTimeMillis();
        ZipOutputStream zos = null ;
        try {
            zos = new ZipOutputStream(out);
            File sourceFile = new File(srcDir);
            compress(sourceFile,zos,sourceFile.getName(), 0);
            long end = System.currentTimeMillis();
            System.out.println("压缩完成，耗时：" + (end - start) +" ms");
        } catch (Exception e) {
            throw new RuntimeException("zip error from ZipUtils",e);
        }finally{
            if(zos != null){
                try {
                    zos.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public String Parse_Command(String request) throws IOException {
        request=request.replace("\n","");
        String res="";
        String[] cmd=request.split("@");
        System.out.println(cmd[0]);
        if (!cmd[0].equals("add"))
        {
            System.out.println(request);
        }
        if (cmd[0].equals("register")) // 注册
        {
            System.out.println("注册");
            File dir = new File(root_path);
            File[] repositories = dir.listFiles();
            int len=repositories.length;

            File new_user=new File(root_path+"/"+Integer.toString(len)+"/");
            new_user.mkdir();
            File user_name=new File(new_user+"/username.txt");
            user_name.createNewFile();
            BufferedWriter out = new BufferedWriter(new FileWriter(user_name));
            out.write(cmd[1]);
            out.close();

            File password=new File(new_user+"/password.txt");
            password.createNewFile();
            out = new BufferedWriter(new FileWriter(password));
            out.write(cmd[2]);
            out.close();

            res=res+Integer.toString(len);
            res=res+","+cmd[2];
        }
        else if (cmd[0].equals("login")) // 登录
        {
            System.out.println("登录");
            String user_id=cmd[1];
            String password=cmd[2];
            File password_file = new File(root_path+user_id+"/"+"password.txt");
            if (!password_file.exists()){
                System.out.println("不存在该用户");
                res=res+"error"+",0";
            }
            else{
                BufferedReader in = new BufferedReader(new FileReader(password_file));
                String str;
                if ((str=in.readLine()).equals(password)){
                    System.out.println("登录成功");
                    res=res+"success";
                    if (m_users.containsKey(user_id))
                    {
                        in.close();
                        return "error,2";
                    }
                    User login_user=new User(user_id);
                    m_users.put(user_id,login_user);
                }
                else {
                    System.out.println("密码错误");
                    res=res+"error"+",1";
                }
                in.close();
            }
        }
        else if (cmd[0].equals("logout")) // 登出
        {
            System.out.println("登出");
            String user_id=cmd[1];
            User logout_user;
            if (m_users.containsKey(user_id))
                logout_user=m_users.get(user_id);
            else
                return "error";
            int len=logout_user.using_datasets.size();
            for (int i=0;i<len;i++)
            {
                String repository_name=logout_user.using_datasets.get(i).repository_name;
                int version_id=logout_user.using_datasets.get(i).version_id;
                HashMap<Integer, Dataset> tmp_hash=new HashMap<Integer, Dataset>();
                if (m_datasets.containsKey(repository_name))
                    tmp_hash=m_datasets.get(repository_name);
                else
                {
                    res="error";
                    return res;
                }
                Dataset tmp;
                if (tmp_hash.containsKey(version_id))
                    tmp=tmp_hash.get(version_id);
                else
                {
                    res="error";
                    return res;
                }
                tmp.lock_user_id="";
                tmp_hash.put(version_id,tmp);
                m_datasets.put(repository_name,tmp_hash);
            }
            m_users.remove(user_id);
            res="success";
        }
        else if (cmd[0].equals("repositories"))   //获取仓库列表
        {
            System.out.println(1);
            String user_id=cmd[1];
            File dir = new File(root_path+user_id+"/");
            File[] repositories = dir.listFiles();
            int len=repositories.length;
            res=res+Integer.toString(len);
            for (int i=0;i<len;i++)
            {
                String file_name=repositories[i].getName();
                if (file_name.equals("username.txt") || file_name.equals("password.txt")){
                    continue;
                }
                res=res+","+file_name;
            }
            //System.out.println(res);
        }
        else if (cmd[0].equals("view_repositories"))
        {
            System.out.println("获取仓库列表界面翻页");
            String user_id=cmd[1];
            int l=Integer.parseInt(cmd[2]),r=Integer.parseInt(cmd[3]);
            File dir = new File(root_path+user_id+"/");
            File[] repositories = dir.listFiles();
            int len=repositories.length,cnt=0;
            res=res+Integer.toString(len);
            for (int i=0;i<len;i++)
            {
                String file_name=repositories[i].getName();
                if (file_name.equals("username.txt") || file_name.equals("password.txt")){
                    continue;
                }
                cnt+=1;
                if (l<=cnt && r>=cnt){
                    res=res+","+file_name;
                }
                if (cnt>r) break;
            }
        }
        else if (cmd[0].equals("repository"))
        {
            System.out.println(2);
            File version_tree = new File(root_path+cmd[1]+"/"+cmd[2]+"/version_tree.txt");
            List<String> version_info=new ArrayList<>();
            try {
                BufferedReader in = new BufferedReader(new FileReader(version_tree));
                String str;
                while((str = in.readLine()) != null){
                    version_info.add(str);
                    //System.out.println(str);
                }
                in.close();
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
            int len=version_info.size();
            String v_tree="";
            v_tree=v_tree+"[";
            for (int i=0;i<len-1;i++)
            {
                v_tree=v_tree+version_info.get(i)+",";
            }
            v_tree=v_tree+version_info.get(len-1);
            v_tree=v_tree+"]";
            int data_type=get_data_type(cmd[1]+"/"+cmd[2]);
            res=res+Integer.toString(data_type)+"@"+Integer.toString(len)+"@"+v_tree;
            //System.out.println(res);
        }
        else if (cmd[0].equals("enable")) // 赋予修改权限
        {
            String user_id=cmd[1];
            String repository_id=cmd[2];
            String cur_user=cmd[3];
            int accept_or_not=Integer.parseInt(cmd[4]);

            String apply_path=root_path+user_id+"/"+repository_id+"/"+"apply_list.txt";
            File apply_list=new File(apply_path);
            BufferedReader br=new BufferedReader(new FileReader(apply_list));
            String temp;
            int flag=1;
            List<String> applies=new ArrayList<>();
            while((temp=br.readLine())!=null)
            {
                if (temp.equals(cur_user))
                {
                    //System.out.println("equal！！");
                    flag=0;
                }
                else
                {
                    applies.add(temp);
                }
            }
            br.close();
            BufferedWriter bw=new BufferedWriter(new FileWriter(apply_path));
            int len=applies.size();
            for (int i=0;i<len;i++)
            {
                bw.write(applies.get(i)+"\n");
            }
            bw.close();

            if (accept_or_not==1) {
                System.out.println("赋予修改权限");
                String repository_name = cmd[1] + "/" + cmd[2];
                File enable_list_file = new File(root_path + repository_name + "/" + "enable_list.txt");

                BufferedReader in = new BufferedReader(new FileReader(enable_list_file));
                String str = in.readLine();
                in.close();
                String[] enable_list = str.split(",");

                File dir = new File(root_path);
                File[] repositories = dir.listFiles();
                int a_len = repositories.length; // 当前用户ID最大值+1

                BufferedWriter out = new BufferedWriter(new FileWriter(enable_list_file, true));
                Set<String> tmp_set = new HashSet<String>(Arrays.asList(enable_list));
                if (isInteger(cur_user)) {
                    int num = Integer.parseInt(cur_user);
                    if (num < 0 || num >= a_len) {
                    }
                    if (tmp_set.contains(cur_user)) {
                    } else {
                        out.write("," + cur_user);
                    }
                }
                out.close();
            }
        }
        else if (cmd[0].equals("check_permission"))
        {
            String repository_name=cmd[1]+"/"+cmd[2];
            File enable_list_file = new File(root_path+repository_name+"/"+"enable_list.txt");
            BufferedReader in = new BufferedReader(new FileReader(enable_list_file));
            String str=in.readLine();
            in.close();

            String[] enable_list=str.split(",");
            Set<String> tmp_set = new HashSet<String>(Arrays.asList(enable_list));
            if (!tmp_set.contains(cmd[3])){
                res=res+"readonly";
            }
            else
                res=res+"readwrite";
        }
        else if (cmd[0].equals("apply_permission"))
        {
            String user_id=cmd[1];
            String repository_name=cmd[2];
            String cur_user=cmd[3];
            String apply_path=root_path+user_id+"/"+repository_name+"/"+"apply_list.txt";
            File apply_list=new File(apply_path);
            BufferedReader br=new BufferedReader(new FileReader(apply_list));
            String temp;
            int flag=1;
            while((temp=br.readLine())!=null)
            {
                System.out.println(temp.length()+" "+cur_user.length());
                if (temp.equals(cur_user))
                {
                    System.out.println("equal！！");
                    flag=0;
                    break;
                }
            }
            br.close();
            if (flag==1)
            {
                BufferedWriter bw=new BufferedWriter(new OutputStreamWriter(new FileOutputStream(apply_path,true)));
                bw.write(cur_user+"\n");
                bw.close();
            }
        }
        else if (cmd[0].equals("view_permission"))
        {
            String user_id=cmd[1];
            String repository_name=cmd[2];
            String apply_path=root_path+user_id+"/"+repository_name+"/"+"apply_list.txt";
            File apply_list=new File(apply_path);
            BufferedReader br=new BufferedReader(new FileReader(apply_list));
            String temp;
            int flag=1;
            while((temp=br.readLine())!=null)
            {
                res=res+temp+"@";
            }
            br.close();
        }
        else if (cmd[0].equals("create"))     //创建仓库
        {
            int data_type=Integer.parseInt(cmd[1]);
            String repository_name=cmd[2]+"/"+cmd[3];
            File repository_dir=new File(root_path+repository_name+"/");
            repository_dir.mkdir();
            File type_file=new File(root_path+repository_name+"/inf.txt");
            if (!type_file.exists())
            {
                type_file.createNewFile();
            }
            File version_tree_file=new File(root_path+repository_name+"/version_tree.txt");
            if (!version_tree_file.exists())
            {
                version_tree_file.createNewFile();
                BufferedWriter out=new BufferedWriter(new OutputStreamWriter(
                        new FileOutputStream(root_path+repository_name+"/version_tree.txt",true)));
                String new_info="{ id:"+Integer.toString(1)+", pId:"
                        +Integer.toString(0)+", name:\"版本"+Integer.toString(1)+"\"}\n";
                out.write(new_info);
                out.close();
            }


            File enable_list_file = new File(root_path+repository_name+"/"+"enable_list.txt");
            if (!enable_list_file.exists()){
                enable_list_file.createNewFile();
                BufferedWriter out = new BufferedWriter(new FileWriter(enable_list_file));
                out.write(cmd[2]);
                out.close();
            }
            File apply_list_file = new File(root_path+repository_name+"/"+"apply_list.txt");
            if (!apply_list_file.exists()){
                apply_list_file.createNewFile();
            }
            BufferedWriter out = new BufferedWriter(new FileWriter(type_file));
            out.write(Integer.toString(data_type));
            out.close();
            String version1_path=root_path+repository_name+"/version1/";
            File version1_dir=new File(version1_path);
            version1_dir.mkdir();
            if (data_type==2)
            {
                File table_dir=new File(version1_path+"data.csv");
                table_dir.createNewFile();
                out = new BufferedWriter(new FileWriter(table_dir));
                String head="";
                for (int i=4;i<cmd.length-1;i++)
                {
                    head=head+cmd[i]+",";
                }
                head=head+cmd[cmd.length-1]+"\n";
                out.write(head);
                out.close();
            }
            else
            {
                if (data_type==0)
                {
                    File pic_dir=new File(version1_path+"pictures/");
                    pic_dir.mkdir();
                    File label_dir=new File(version1_path+"labels/");
                    label_dir.mkdir();
                }
                else
                {
                    File text_dir=new File(version1_path+"texts/");
                    text_dir.mkdir();
                    File label_dir=new File(version1_path+"labels/");
                    label_dir.mkdir();
                }
                try {
                    File fs = new File(version1_path+"index.txt");
                    fs.createNewFile();
                    BufferedWriter f_out = new BufferedWriter(new FileWriter(fs));
                    f_out.write(Integer.toString(1));
                    f_out.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        else if (cmd[0].equals("enter"))      //进入仓库
        {
            String user_id=cmd[1];
            String repository_name=cmd[2];
            int version_id=Integer.parseInt(cmd[3])+1;
            int data_type=get_data_type(user_id+"/"+repository_name);
            Dataset tmp;
            if (data_type==0)
            {
                tmp=new Picture_Dataset(user_id,repository_name,version_id);
            }
            else if (data_type==1)
            {
                tmp=new Text_Dataset(user_id,repository_name,version_id);
            }
            else{
                tmp=new Table_Dataset(user_id,repository_name,version_id);
            }
            HashMap<Integer, Dataset> tmp_hash;
            if (m_datasets.containsKey(repository_name)) {
                tmp_hash = m_datasets.get(repository_name);
                if (tmp_hash.containsKey(version_id))
                {

                }
                else
                {
                    tmp_hash.put(version_id,tmp);
                }
            }
            else
            {
                tmp_hash = new HashMap<Integer, Dataset>();
                tmp_hash.put(version_id,tmp);
            }
            m_datasets.put(repository_name,tmp_hash);
            res="";
            if (data_type==0)
            {
                //responsetype=1;
                String picture_path = root_path+user_id+"/"+repository_name+"/version"+ version_id + "/pictures/";
                String label_path = root_path+user_id+"/"+repository_name+"/version"+ version_id + "/labels/";
                System.out.println(picture_path);
                File picture_dir = new File(picture_path);
                File pictures[] = picture_dir.listFiles();
                res=Integer.toString(pictures.length);
                /*for (int i = 0; i < pictures.length; i++) {
                    File fs = pictures[i];
                    if (fs.isFile()) {
                        try {
                            byte[] res_buf;
                            BufferedImage bimg =ImageIO.read(fs);
                            //bais.close();
                            BufferedImage updateImage=new BufferedImage(200,200, BufferedImage.TYPE_INT_RGB);
                            updateImage.getGraphics().drawImage(bimg, 0, 0, 200, 200, null);
                            ByteArrayOutputStream baos = new ByteArrayOutputStream();
                            ImageIO.write(updateImage, "jpg", baos);
                            baos.flush();
                            res_buf=baos.toByteArray();
                            baos.close();
                            res=res+Base64.getEncoder().encodeToString(res_buf)+"@";
                        } catch (IOException e) {
                            System.out.println("Wrong!");
                        }
                    }
                }
                File label_dir = new File(label_path);
                File labels[] = label_dir.listFiles();
                for (int i = 0; i < labels.length; i++) {
                    File fs = labels[i];
                    if (fs.isFile()) {
                        try {
                            BufferedReader in = new BufferedReader(new FileReader(fs));
                            String s="",temp;
                            while((temp=in.readLine())!=null)
                            {
                                s=s+temp;
                            }
                            System.out.println(s);
                            res=res+s+"@";
                            in.close();
                        } catch (IOException e) {
                            System.out.println("Wrong!");
                        }
                    }
                }*/
                //tmp=new Text_Dataset(repository_name,version_id);
            }
            else if (data_type==1)
            {
                String text_path = root_path+user_id+"/"+repository_name+"/version"+Integer.toString(version_id)+ "/texts/";
                String label_path = root_path+user_id+"/"+repository_name+"/version"+Integer.toString(version_id)+ "/labels/";
                System.out.println(text_path);
                File text_dir = new File(text_path);
                File texts[] = text_dir.listFiles();
                res=Integer.toString(texts.length);
                /*for (int i = 0; i < texts.length; i++) {
                    File fs = texts[i];
                    if (fs.isFile()) {
                        try {
                            BufferedReader in = new BufferedReader(new FileReader(fs));
                            String s="",temp;
                            while((temp=in.readLine())!=null)
                            {
                                s=s+temp;
                            }
                            System.out.println(s);
                            res=res+s+"@";
                            in.close();
                        } catch (IOException e) {
                            System.out.println("Wrong!");
                        }
                    }
                }
                File label_dir = new File(label_path);
                File labels[] = label_dir.listFiles();
                for (int i = 0; i < labels.length; i++) {
                    File fs = labels[i];
                    if (fs.isFile()) {
                        try {
                            BufferedReader in = new BufferedReader(new FileReader(fs));
                            String s="",temp;
                            while((temp=in.readLine())!=null)
                            {
                                s=s+temp;
                            }
                            System.out.println(s);
                            res=res+s+"@";
                            in.close();
                        } catch (IOException e) {
                            System.out.println("Wrong!");
                        }
                    }
                }*/
                //tmp=new Text_Dataset(repository_name,version_id);
            }
            else{
                String table_path = root_path+user_id+"/"+repository_name+"/version"+Integer.toString(version_id)+ "/data.csv";
                System.out.println(table_path);
                File table_dir = new File(table_path);
                try {
                    BufferedReader in = new BufferedReader(new FileReader(table_dir));
                    String str;
                    while((str = in.readLine()) != null){
                        res=res+str+"\n";
                    }
                    in.close();
                }
                catch(Exception e)
                {
                    e.printStackTrace();
                }
                System.out.println(res);
            }
            //System.out.println(res);
        }
        else if (cmd[0].equals("view_dataset"))      //获取数据集翻页
        {
            System.out.println("获取数据集翻页");
            String user_id=cmd[1];
            String repository_name=cmd[2];
            int version_id=Integer.parseInt(cmd[3])+1;
            int l=Integer.parseInt(cmd[4]),r=Integer.parseInt(cmd[5]);
            int data_type=get_data_type(user_id+"/"+repository_name);
            Dataset tmp;
            if (data_type==0)
            {
                tmp=new Picture_Dataset(user_id,repository_name,version_id);
            }
            else if (data_type==1)
            {
                tmp=new Text_Dataset(user_id,repository_name,version_id);
            }
            else{
                tmp=new Table_Dataset(user_id,repository_name,version_id);
            }
            HashMap<Integer, Dataset> tmp_hash;
            if (m_datasets.containsKey(repository_name)) {
                tmp_hash = m_datasets.get(repository_name);
                if (tmp_hash.containsKey(version_id))
                {

                }
                else
                {
                    tmp_hash.put(version_id,tmp);
                }
            }
            else
            {
                tmp_hash = new HashMap<Integer, Dataset>();
                tmp_hash.put(version_id,tmp);
            }
            m_datasets.put(repository_name,tmp_hash);
            res="";
            if (data_type==0)
            {
                //responsetype=1;
                String picture_path = root_path+user_id+"/"+repository_name+"/version"+Integer.toString(version_id)+ "/pictures/";
                String label_path = root_path+user_id+"/"+repository_name+"/version"+Integer.toString(version_id)+ "/labels/";
                System.out.println(picture_path);
                File picture_dir = new File(picture_path);
                File pictures[] = picture_dir.listFiles();
                int cnt=0;
                for (int i = 0; i < pictures.length; i++) {
                    File fs = pictures[i];
                    cnt+=1;
                    if (fs.isFile()) {
                        if (l<=cnt && r>=cnt){
                            try {
                                byte[] res_buf;
                                BufferedImage bimg =ImageIO.read(fs);
                                //bais.close();
                                BufferedImage updateImage=new BufferedImage(200,200, BufferedImage.TYPE_INT_RGB);
                                updateImage.getGraphics().drawImage(bimg, 0, 0, 200, 200, null);
                                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                                ImageIO.write(updateImage, "jpg", baos);
                                baos.flush();
                                res_buf=baos.toByteArray();
                                baos.close();
                                res=res+Base64.getEncoder().encodeToString(res_buf)+"@";
                            } catch (IOException e) {
                                System.out.println("Wrong!");
                            }
                        }
                    }
                }
                cnt=0;
                File label_dir = new File(label_path);
                File labels[] = label_dir.listFiles();
                for (int i = 0; i < labels.length; i++) {
                    File fs = labels[i];
                    cnt+=1;
                    if (fs.isFile()) {
                        if (l<=cnt && r>=cnt){
                            try {
                                BufferedReader in = new BufferedReader(new FileReader(fs));
                                String s="",temp;
                                while((temp=in.readLine())!=null)
                                {
                                    s=s+temp;
                                }
                                System.out.println(s);
                                res=res+s+"@";
                                in.close();
                            } catch (IOException e) {
                                System.out.println("Wrong!");
                            }
                        }
                    }
                }
                //tmp=new Text_Dataset(repository_name,version_id);
            }
            else if (data_type==1)
            {
                String text_path = root_path+user_id+"/"+repository_name+"/version"+Integer.toString(version_id)+ "/texts/";
                String label_path = root_path+user_id+"/"+repository_name+"/version"+Integer.toString(version_id)+ "/labels/";
                System.out.println(text_path);
                File text_dir = new File(text_path);
                File texts[] = text_dir.listFiles();
                int cnt=0;
                for (int i = 0; i < texts.length; i++) {
                    File fs = texts[i];
                    cnt+=1;
                    if (fs.isFile()) {
                        if (l<=cnt && r>=cnt) {
                            try {
                                BufferedReader in = new BufferedReader(new FileReader(fs));
                                String s = "", temp;
                                while ((temp = in.readLine()) != null) {
                                    s = s + temp;
                                }
                                System.out.println(s);
                                res = res + s + "@";
                                in.close();
                            } catch (IOException e) {
                                System.out.println("Wrong!");
                            }
                        }
                    }
                }
                File label_dir = new File(label_path);
                File labels[] = label_dir.listFiles();
                cnt=0;
                for (int i = 0; i < labels.length; i++) {
                    File fs = labels[i];
                    cnt+=1;
                    if (fs.isFile()) {
                        if (l<=cnt && r>=cnt) {
                            try {
                                BufferedReader in = new BufferedReader(new FileReader(fs));
                                String s = "", temp;
                                while ((temp = in.readLine()) != null) {
                                    s = s + temp;
                                }
                                System.out.println(s);
                                res = res + s + "@";
                                in.close();
                            } catch (IOException e) {
                                System.out.println("Wrong!");
                            }
                        }
                    }
                }
                //tmp=new Text_Dataset(repository_name,version_id);
            }
            else{
                String table_path = root_path+user_id+"/"+repository_name+"/version"+Integer.toString(version_id)+ "/data.csv";
                System.out.println(table_path);
                File table_dir = new File(table_path);
                int cnt=0;
                try {
                    BufferedReader in = new BufferedReader(new FileReader(table_dir));
                    String str;
                    if ((str = in.readLine())!=null){
                        res=res+str+"\n";
                    }
                    while((str = in.readLine()) != null){
                        cnt+=1;
                        if (l<=cnt && r>=cnt){
                            res=res+str+"\n";
                        }
                    }
                    in.close();
                }
                catch(Exception e)
                {
                    e.printStackTrace();
                }
                System.out.println(res);
            }
            //System.out.println(res);
        }
        else if (cmd[0].equals("enter_tmp"))      //进入仓库
        {
            String user_id=cmd[1];
            String repository_name=cmd[2];
            int version_id=Integer.parseInt(cmd[3])+1;
            int data_type=get_data_type(user_id+"/"+repository_name);
            HashMap<Integer, Dataset> tmp_hash=m_datasets.get(repository_name);
            if (data_type==0)
            {
                Picture_Dataset tmp=(Picture_Dataset)m_datasets.get(repository_name).get(version_id);
                tmp.reload(0);
                String picture_path = root_path+user_id+"/"+repository_name+"/version"+Integer.toString(version_id)+ "/tmp_pictures/";
                String label_path = root_path+user_id+"/"+repository_name+"/version"+Integer.toString(version_id)+ "/tmp_labels/";
                System.out.println(picture_path);
                File picture_dir = new File(picture_path);
                File pictures[] = picture_dir.listFiles();
                res=Integer.toString(pictures.length);
                /*for (int i = 0; i < pictures.length; i++) {
                    File fs = pictures[i];
                    if (fs.isFile()) {
                        try {
                            byte[] res_buf;
                            BufferedImage bimg =ImageIO.read(fs);
                            //bais.close();
                            BufferedImage updateImage=new BufferedImage(200,200, BufferedImage.TYPE_INT_RGB);
                            updateImage.getGraphics().drawImage(bimg, 0, 0, 200, 200, null);
                            ByteArrayOutputStream baos = new ByteArrayOutputStream();
                            ImageIO.write(updateImage, "jpg", baos);
                            baos.flush();
                            res_buf=baos.toByteArray();
                            baos.close();
                            res=res+Base64.getEncoder().encodeToString(res_buf)+"@";
                        } catch (IOException e) {
                            System.out.println("Wrong!");
                        }
                    }
                }
                File label_dir = new File(label_path);
                File labels[] = label_dir.listFiles();
                for (int i = 0; i < labels.length; i++) {
                    File fs = labels[i];
                    if (fs.isFile()) {
                        try {
                            BufferedReader in = new BufferedReader(new FileReader(fs));
                            String s = "", temp;
                            while ((temp = in.readLine()) != null) {
                                s = s + temp;
                            }
                            System.out.println(s);
                            res = res + s + "@";
                            in.close();
                        } catch (IOException e) {
                            System.out.println("Wrong!");
                        }
                    }
                }*/
                tmp_hash.put(version_id,tmp);
            }
            else if (data_type==1)
            {
                Text_Dataset tmp=(Text_Dataset)m_datasets.get(repository_name).get(version_id);
                //System.out.println("hhhhh");
                tmp.reload(0);
                String text_path = root_path+user_id+"/"+repository_name+"/version"+Integer.toString(version_id)+ "/tmp_texts/";
                String label_path = root_path+user_id+"/"+repository_name+"/version"+Integer.toString(version_id)+ "/tmp_labels/";
                System.out.println(text_path);
                File text_dir = new File(text_path);
                File texts[] = text_dir.listFiles();
                res=Integer.toString(texts.length);
                /*for (int i = 0; i < texts.length; i++) {
                    File fs = texts[i];
                    if (fs.isFile()) {
                        try {
                            BufferedReader in = new BufferedReader(new FileReader(fs));
                            String s="",temp;
                            while((temp=in.readLine())!=null)
                            {
                                s=s+temp;
                            }
                            System.out.println(s);
                            res=res+s+"@";
                            in.close();
                        } catch (IOException e) {
                            System.out.println("Wrong!");
                        }
                    }
                }
                File label_dir = new File(label_path);
                File labels[] = label_dir.listFiles();
                for (int i = 0; i < labels.length; i++) {
                    File fs = labels[i];
                    if (fs.isFile()) {
                        try {
                            BufferedReader in = new BufferedReader(new FileReader(fs));
                            String s="",temp;
                            while((temp=in.readLine())!=null)
                            {
                                s=s+temp;
                            }
                            System.out.println(s);
                            res=res+s+"@";
                            in.close();
                        } catch (IOException e) {
                            System.out.println("Wrong!");
                        }
                    }
                }
                System.out.println("TMPRES"+res);*/
                tmp_hash.put(version_id,tmp);
            }
            else {
                Table_Dataset tmp = (Table_Dataset) m_datasets.get(repository_name).get(version_id);
                tmp.reload(0);
                String table_path = root_path + user_id+"/"+repository_name + "/version" + Integer.toString(version_id) + "/tmp_data.csv";
                System.out.println(table_path);
                File table_dir = new File(table_path);
                try {
                    BufferedReader in = new BufferedReader(new FileReader(table_dir));
                    String str;
                    while ((str = in.readLine()) != null) {
                        res = res + str + "\n";
                    }
                    in.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                System.out.println(res);
                tmp_hash.put(version_id, tmp);
            }
            m_datasets.put(repository_name,tmp_hash);

        }
        else if (cmd[0].equals("view_tmp_dataset"))      //进入仓库
        {
            String user_id=cmd[1];
            String repository_name=cmd[2];
            int version_id=Integer.parseInt(cmd[3])+1;
            int data_type=get_data_type(user_id+"/"+repository_name);
            int l=Integer.parseInt(cmd[4]),r=Integer.parseInt(cmd[5]);
            HashMap<Integer, Dataset> tmp_hash=m_datasets.get(repository_name);
            if (data_type==0)
            {
                Picture_Dataset tmp=(Picture_Dataset)m_datasets.get(repository_name).get(version_id);
                //tmp.reload(tmp.op.ordinal());
                String picture_path = root_path+user_id+"/"+repository_name+"/version"+Integer.toString(version_id)+ "/tmp_pictures/";
                String label_path = root_path+user_id+"/"+repository_name+"/version"+Integer.toString(version_id)+ "/tmp_labels/";
                System.out.println(picture_path);
                File picture_dir = new File(picture_path);
                File pictures[] = picture_dir.listFiles();
                int cnt=0;
                for (int i = 0; i < pictures.length; i++) {
                    File fs = pictures[i];
                    cnt+=1;
                    if (fs.isFile()) {
                        if (l<=cnt && r>=cnt) {
                            try {
                                byte[] res_buf;
                                BufferedImage bimg = ImageIO.read(fs);
                                //bais.close();
                                BufferedImage updateImage = new BufferedImage(200, 200, BufferedImage.TYPE_INT_RGB);
                                updateImage.getGraphics().drawImage(bimg, 0, 0, 200, 200, null);
                                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                                ImageIO.write(updateImage, "jpg", baos);
                                baos.flush();
                                res_buf = baos.toByteArray();
                                baos.close();
                                res = res + Base64.getEncoder().encodeToString(res_buf) + "@";
                            } catch (IOException e) {
                                System.out.println("Wrong!");
                            }
                        }
                    }
                }
                cnt=0;
                File label_dir = new File(label_path);
                File labels[] = label_dir.listFiles();
                for (int i = 0; i < labels.length; i++) {
                    File fs = labels[i];
                    cnt+=1;
                    if (fs.isFile()) {
                        if (l<=cnt && r>=cnt) {
                            try {
                                BufferedReader in = new BufferedReader(new FileReader(fs));
                                String s = "", temp;
                                while ((temp = in.readLine()) != null) {
                                    s = s + temp;
                                }
                                //System.out.println(s);
                                res = res + s + "@";
                                in.close();
                            } catch (IOException e) {
                                System.out.println("Wrong!");
                            }
                        }
                    }
                }
                tmp_hash.put(version_id,tmp);
            }
            else if (data_type==1)
            {
                Text_Dataset tmp=(Text_Dataset)m_datasets.get(repository_name).get(version_id);
                //System.out.println("hhhhh");
                //tmp.reload(tmp.op.ordinal());
                String text_path = root_path+user_id+"/"+repository_name+"/version"+Integer.toString(version_id)+ "/tmp_texts/";
                String label_path = root_path+user_id+"/"+repository_name+"/version"+Integer.toString(version_id)+ "/tmp_labels/";
                System.out.println(text_path);
                File text_dir = new File(text_path);
                File texts[] = text_dir.listFiles();
                int cnt=0;
                for (int i = 0; i < texts.length; i++) {
                    File fs = texts[i];
                    cnt+=1;
                    if (fs.isFile()) {
                        if (l <= cnt && r >= cnt) {
                            try {
                                BufferedReader in = new BufferedReader(new FileReader(fs));
                                String s = "", temp;
                                while ((temp = in.readLine()) != null) {
                                    s = s + temp;
                                }
                                //System.out.println(s);
                                res = res + s + "@";
                                in.close();
                            } catch (IOException e) {
                                System.out.println("Wrong!");
                            }
                        }
                    }
                }
                File label_dir = new File(label_path);
                File labels[] = label_dir.listFiles();
                cnt=0;
                for (int i = 0; i < labels.length; i++) {
                    File fs = labels[i];
                    cnt+=1;
                    if (fs.isFile()) {
                        if (l<=cnt && r>=cnt) {
                            try {
                                BufferedReader in = new BufferedReader(new FileReader(fs));
                                String s = "", temp;
                                while ((temp = in.readLine()) != null) {
                                    s = s + temp;
                                }
                                //System.out.println(s);
                                res = res + s + "@";
                                in.close();
                            } catch (IOException e) {
                                System.out.println("Wrong!");
                            }
                        }
                    }
                }
                System.out.println("TMPRES"+res);
                tmp_hash.put(version_id,tmp);
            }
            else {
                Table_Dataset tmp = (Table_Dataset) m_datasets.get(repository_name).get(version_id);
                //tmp.reload(tmp.op.ordinal());
                String table_path = root_path + user_id+"/"+repository_name + "/version" + Integer.toString(version_id) + "/tmp_data.csv";
                System.out.println(table_path);
                File table_dir = new File(table_path);
                int cnt=0;
                try {
                    BufferedReader in = new BufferedReader(new FileReader(table_dir));
                    String str;
                    if ((str = in.readLine()) != null) {
                        res = res + str + "\n";
                    }
                    while ((str = in.readLine()) != null) {
                        cnt+=1;
                        if (l<=cnt && r>=cnt) {
                            res = res + str + "\n";
                        }
                    }
                    in.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                System.out.println(res);
                tmp_hash.put(version_id, tmp);
            }
            m_datasets.put(repository_name,tmp_hash);

        }
        else if (cmd[0].equals("reset"))     //删除数据
        {
            String user_id=cmd[1];
            String repository_name=cmd[2];
            int version_id=Integer.parseInt(cmd[3])+1;
            int data_type=get_data_type(user_id+"/"+repository_name);
            HashMap<Integer, Dataset> tmp_hash=m_datasets.get(repository_name);
            if (data_type==0)
            {
                Picture_Dataset tmp=(Picture_Dataset)m_datasets.get(repository_name).get(version_id);
                tmp.load(0);
                tmp_hash.put(version_id,tmp);
            }
            else if (data_type==1)
            {
                Text_Dataset tmp=(Text_Dataset)m_datasets.get(repository_name).get(version_id);
                tmp.load(0);
                tmp_hash.put(version_id,tmp);
            }
            else
            {
                Table_Dataset tmp=(Table_Dataset)m_datasets.get(repository_name).get(version_id);
                tmp.load(0);
                tmp_hash.put(version_id,tmp);
            }
            m_datasets.put(repository_name,tmp_hash);
        }
        else if (cmd[0].equals("lock"))
        {
            String user_id=cmd[1];
            String repository_name=cmd[2];
            int version_id=Integer.parseInt(cmd[3])+1;
            String cur_user=cmd[4];
            HashMap<Integer, Dataset> tmp_hash=m_datasets.get(repository_name);
            Dataset tmp=m_datasets.get(repository_name).get(version_id);
            System.out.println("Lockid:"+tmp.lock_user_id);
            if (tmp.lock_user_id.equals(""))
            {
                tmp.lock_user_id=cur_user;
                User tmp_usr=m_users.get(cur_user);
                tmp_usr.use_datasets(tmp);
                m_users.put(cur_user,tmp_usr);
                res="ok";
            }
            else if (tmp.lock_user_id.equals(cur_user))
            {
                res="ok";
            }
            else
                res="wrong";
            System.out.println(res);
            tmp_hash.put(version_id,tmp);
            m_datasets.put(repository_name,tmp_hash);
        }
        else if (cmd[0].equals("add"))        //增加数据
        {
            String user_id=cmd[1];
            String repository_name=cmd[2];
            int version_id=Integer.parseInt(cmd[3])+1;
            int data_type=get_data_type(user_id+"/"+repository_name);
            HashMap<Integer, Dataset> tmp_hash=m_datasets.get(repository_name);
            if (cmd.length==4)
            {
                if (data_type==0)
                {
                    Picture_Dataset tmp=(Picture_Dataset)m_datasets.get(repository_name).get(version_id);
                    tmp.reload(0);
                    tmp.ready_for_add();
                    tmp_hash.put(version_id,tmp);
                }
                else if (data_type==1)
                {
                    Text_Dataset tmp=(Text_Dataset)m_datasets.get(repository_name).get(version_id);
                    tmp.reload(0);
                    tmp.ready_for_add();
                    tmp_hash.put(version_id,tmp);
                    Text_Dataset tmp1=(Text_Dataset)m_datasets.get(repository_name).get(version_id);
                }
                else
                {
                    Table_Dataset tmp=(Table_Dataset)m_datasets.get(repository_name).get(version_id);
                    tmp.reload(0);
                    tmp.ready_for_add();
                    tmp_hash.put(version_id,tmp);
                }
                m_datasets.put(repository_name,tmp_hash);
            }
            else
            {
                String content=cmd[4];
                if (data_type==0)
                {
                    byte[] res_buf;
                    res_buf=Base64.getDecoder().decode(content);
                    //System.out.println("yes");
                    //int label=Integer.parseInt(cmd[4]);
                    InputStream input = new ByteArrayInputStream(res_buf);
                    ZipInputStream zipInputStream = new ZipInputStream(input);
                    ZipEntry nextEntry = zipInputStream.getNextEntry();
                    List<byte[]> add_contents=new ArrayList<>();
                    //byte [][]add_contents=new byte[1024][];
                    List<String> add_labels=new ArrayList<>();
                    //不为空进入循环
                    while (nextEntry != null) {
                        String name = nextEntry.getName();
                        System.out.println(name);
                        //如果是目录，创建目录
                        if (name.endsWith("/")) {
                            //file.mkdir();
                        } else {
                            //文件则写入具体的路径中
                            String inform[]=name.split("/");
                            if (inform[0].equals("labels"))
                            {
                                byte[] bytes=new byte[1024];
                                int len=0;
                                while (true) {
                                    int tmplen=zipInputStream.read(bytes);
                                    if (tmplen==-1)
                                        break;
                                    else
                                        len=tmplen;
                                }
                                //System.out.println(Integer.toString(len));
                                add_labels.add(new String(bytes,0,len));
                                //System.out.println(new String(bytes,0,len));
                            }
                            else{
                                byte[] bytes=new byte[1024];
                                int len=0;
                                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                                while ((len=zipInputStream.read(bytes))!=-1) {
                                    baos.write(bytes,0,len);
                                }
                                //System.out.println(Integer.toString(len));
                                //add_contents.add(baos.toByteArray());
                                ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(baos.toByteArray());
                                //BufferedImage bimg =ImageIO.read(byteArrayInputStream);
                                //BufferedImage updateImage=new BufferedImage(200,200, BufferedImage.TYPE_INT_RGB);
                                //updateImage.getGraphics().drawImage(bimg, 0, 0, 200, 200, null);
                                //ByteArrayOutputStream baos1 = new ByteArrayOutputStream();
                                //ImageIO.write(updateImage, "jpg", baos1);
                                //baos1.flush();
                                add_contents.add(baos.toByteArray());
                                baos.close();
                                //baos1.close();
                                //System.out.println(new String(bytes,0,len));
                            }
                        }
                        //关闭当前布姆
                        zipInputStream.closeEntry();
                        //读取下一个目录，作为循环条件
                        nextEntry = zipInputStream.getNextEntry();
                    }
                    Picture_Dataset tmp=(Picture_Dataset)m_datasets.get(repository_name).get(version_id);
                    int m_len=add_labels.size();
                    res="";
                    for (int i=0;i<m_len;i++)
                    {
                        tmp.add_picture(add_contents.get(i),add_labels.get(i));
                        //res=res+Base64.getEncoder().encodeToString(add_contents.get(i))+"@"+
                                //add_labels.get(i)+"@";
                    }
                    //tmp.add_picture(resbuf,label);
                    tmp_hash.put(version_id,tmp);
                }
                else if (data_type==1)
                {
                    byte[] res_buf;
                    res_buf=Base64.getDecoder().decode(content);
                    //System.out.println("yes");
                    //int label=Integer.parseInt(cmd[4]);
                    InputStream input = new ByteArrayInputStream(res_buf);
                    ZipInputStream zipInputStream = new ZipInputStream(input);
                    ZipEntry nextEntry = zipInputStream.getNextEntry();
                    List<String> add_contents=new ArrayList<>();
                    //byte [][]add_contents=new byte[1024][];
                    List<String> add_labels=new ArrayList<>();
                    //不为空进入循环
                    while (nextEntry != null) {
                        String name = nextEntry.getName();
                        System.out.println(name);
                        //如果是目录，创建目录
                        if (name.endsWith("/")) {
                            //file.mkdir();
                        } else {
                            //文件则写入具体的路径中
                            String inform[]=name.split("/");
                            if (inform[0].equals("labels"))
                            {
                                byte[] bytes=new byte[1024];
                                int len=0;
                                while (true) {
                                    int tmplen=zipInputStream.read(bytes);
                                    if (tmplen==-1)
                                        break;
                                    else
                                        len+=tmplen;
                                }
                                //System.out.println(Integer.toString(len));
                                add_labels.add(new String(bytes,0,len));
                                //System.out.println(new String(bytes,0,len));
                            }
                            else{
                                byte[] bytes=new byte[1024];
                                int len=0;
                                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                                while (true) {
                                    int tmplen=zipInputStream.read(bytes);
                                    if (tmplen==-1)
                                        break;
                                    else
                                    {
                                        baos.write(bytes,0,tmplen);
                                        len += tmplen;
                                    }
                                }
                                //System.out.println(Integer.toString(len));
                                add_contents.add(baos.toString());
                                //System.out.println(baos.toString());
                                //baos.close();
                                //System.out.println(new String(bytes,0,len));
                            }
                        }
                        //关闭当前布姆
                        zipInputStream.closeEntry();
                        //读取下一个目录，作为循环条件
                        nextEntry = zipInputStream.getNextEntry();
                    }
                    Text_Dataset tmp=(Text_Dataset)m_datasets.get(repository_name).get(version_id);

                    //tmp.add_picture(resbuf,label);
                    //tmp_hash.put(version_id,tmp);
                    int m_len=add_labels.size();
                    res="";
                    for (int i=0;i<m_len;i++)
                    {
                        tmp.add_text(add_contents.get(i),add_labels.get(i));
                        //res=res+add_contents.get(i)+"@"+ add_labels.get(i)+"@";
                    }
                    tmp_hash.put(version_id,tmp);
                }
                else
                {
                    byte [] res_buf;
                    res_buf=Base64.getDecoder().decode(content);
                    InputStream input = new ByteArrayInputStream(res_buf);
                    BufferedReader in=new BufferedReader(new InputStreamReader(input));
                    String str;
                    res="";
                    Table_Dataset tmp=(Table_Dataset)m_datasets.get(repository_name).get(version_id);
                    if ((str = in.readLine()) != null) {
                    }
                    while((str = in.readLine()) != null){
                        //System.out.println(str);
                        //res=res+str+"@";
                        String[] items=str.split(",");
                        tmp.add_row(items);
                    }
                    in.close();
                    tmp_hash.put(version_id,tmp);
                }
                //System.out.println(res);
                m_datasets.put(repository_name,tmp_hash);
            }
        }
        else if (cmd[0].equals("delete"))     //删除数据
        {
            String user_id=cmd[1];
            String repository_name=cmd[2];
            int version_id=Integer.parseInt(cmd[3])+1;
            int data_type=get_data_type(user_id+"/"+repository_name);
            HashMap<Integer, Dataset> tmp_hash=m_datasets.get(repository_name);
            if (cmd.length==4)
            {
                if (data_type==0)
                {
                    Picture_Dataset tmp=(Picture_Dataset)m_datasets.get(repository_name).get(version_id);
                    tmp.reload(1);
                    tmp.ready_for_delete();
                    tmp_hash.put(version_id,tmp);
                }
                else if (data_type==1)
                {
                    Text_Dataset tmp=(Text_Dataset)m_datasets.get(repository_name).get(version_id);
                    tmp.reload(1);
                    tmp.ready_for_delete();
                    tmp_hash.put(version_id,tmp);
                }
                else
                {
                    Table_Dataset tmp=(Table_Dataset)m_datasets.get(repository_name).get(version_id);
                    tmp.reload(1);
                    tmp.ready_for_delete();
                    tmp_hash.put(version_id,tmp);
                }
                m_datasets.put(repository_name,tmp_hash);
            }
            else
            {
                int delete_id=Integer.parseInt(cmd[4])-1;
                if (data_type==0)
                {
                    Picture_Dataset tmp=(Picture_Dataset)m_datasets.get(repository_name).get(version_id);
                    tmp.delete_picture(delete_id);
                    tmp_hash.put(version_id,tmp);
                }
                else if (data_type==1)
                {
                    Text_Dataset tmp=(Text_Dataset)m_datasets.get(repository_name).get(version_id);
                    tmp.delete_text(delete_id);
                    tmp_hash.put(version_id,tmp);
                }
                else
                {
                    Table_Dataset tmp=(Table_Dataset)m_datasets.get(repository_name).get(version_id);
                    tmp.delete_row(delete_id);
                    tmp_hash.put(version_id,tmp);
                }
                m_datasets.put(repository_name,tmp_hash);
            }
        }
        else if (cmd[0].equals("modify"))
        {
            String user_id=cmd[1];
            String repository_name=cmd[2];
            int version_id=Integer.parseInt(cmd[3])+1;
            int data_type=get_data_type(user_id+"/"+repository_name);
            HashMap<Integer, Dataset> tmp_hash=m_datasets.get(repository_name);
            if (cmd.length==4)
            {
                if (data_type==0)
                {
                    Picture_Dataset tmp=(Picture_Dataset)m_datasets.get(repository_name).get(version_id);
                    tmp.reload(2);
                    tmp.ready_for_modified();
                    tmp_hash.put(version_id,tmp);
                }
                else if (data_type==1)
                {
                    Text_Dataset tmp=(Text_Dataset)m_datasets.get(repository_name).get(version_id);
                    tmp.reload(3);
                    tmp.ready_for_modified();
                    tmp_hash.put(version_id,tmp);
                }
                else
                {
                    Table_Dataset tmp=(Table_Dataset)m_datasets.get(repository_name).get(version_id);
                    tmp.reload(2);
                    tmp.ready_for_modified();
                    tmp_hash.put(version_id,tmp);
                }
                m_datasets.put(repository_name,tmp_hash);
            }
            else
            {
                int modify_id=Integer.parseInt(cmd[4])-1;
                if (data_type==0)
                {
                    String label=cmd[5];
                    Picture_Dataset tmp=(Picture_Dataset)m_datasets.get(repository_name).get(version_id);
                    tmp.modify_label(modify_id,label);
                    tmp_hash.put(version_id,tmp);
                }
                else if (data_type==1)
                {
                    String label=cmd[5];
                    Text_Dataset tmp=(Text_Dataset)m_datasets.get(repository_name).get(version_id);
                    tmp.modify_label(modify_id,label);
                    tmp_hash.put(version_id,tmp);
                }
                else
                {
                    String[] items=cmd[5].split(",");
                    Table_Dataset tmp=(Table_Dataset)m_datasets.get(repository_name).get(version_id);
                    tmp.modify_row(modify_id,items);
                    tmp_hash.put(version_id,tmp);
                }
                m_datasets.put(repository_name,tmp_hash);
            }
        }
        else if (cmd[0].equals("modify_text"))  //修改文本数据
        {
            String user_id=cmd[1];
            String repository_name=cmd[2];
            int version_id=Integer.parseInt(cmd[3])+1;
            int data_type=get_data_type(user_id+"/"+repository_name);
            HashMap<Integer, Dataset> tmp_hash=m_datasets.get(repository_name);
            if (cmd.length==4)
            {
                Text_Dataset tmp=(Text_Dataset)m_datasets.get(repository_name).get(version_id);
                tmp.reload(2);
                tmp.ready_for_modified();
                tmp_hash.put(version_id,tmp);
                m_datasets.put(repository_name,tmp_hash);
            }
            else
            {
                int modify_id=Integer.parseInt(cmd[4])-1;
                String content=cmd[5];
                Text_Dataset tmp=(Text_Dataset)m_datasets.get(repository_name).get(version_id);
                tmp.modify_text(modify_id,content);
                tmp_hash.put(version_id,tmp);
                m_datasets.put(repository_name,tmp_hash);
            }
        }
        else if (cmd[0].equals("save"))        //保存数据
        {
            String user_id=cmd[1];
            String repository_name=cmd[2];
            int version_id=Integer.parseInt(cmd[3])+1;
            String cur_user=cmd[4];
            int data_type=get_data_type(user_id+"/"+repository_name);
            HashMap<Integer, Dataset> tmp_hash=m_datasets.get(repository_name);
            User tmp_usr;
            if (m_users.containsKey(cur_user))
                tmp_usr=m_users.get(cur_user);
            else
                return "error";
            if (data_type==0)
            {
                Picture_Dataset tmp=(Picture_Dataset)m_datasets.get(repository_name).get(version_id);
                tmp.save();
                tmp.lock_user_id="";
                tmp_usr.exit_datasets(tmp);
                tmp_hash.put(version_id,tmp);
            }
            else if (data_type==1)
            {
                Text_Dataset tmp=(Text_Dataset)m_datasets.get(repository_name).get(version_id);
                tmp.save();
                tmp.lock_user_id="";
                tmp_usr.exit_datasets(tmp);
                tmp_hash.put(version_id,tmp);
            }
            else
            {
                Table_Dataset tmp = (Table_Dataset) m_datasets.get(repository_name).get(version_id);
                tmp.save();
                tmp.lock_user_id = "";
                tmp_usr.exit_datasets(tmp);
                tmp_hash.put(version_id, tmp);
            }
            m_users.put(cur_user,tmp_usr);
            m_datasets.put(repository_name,tmp_hash);
        }
        else if (cmd[0].equals("submit"))      //提交数据，创建新版本
        {
            String user_id=cmd[1];
            String repository_name=cmd[2];
            int version_id=Integer.parseInt(cmd[3])+1;
            int data_type=get_data_type(user_id+"/"+repository_name);
            HashMap<Integer, Dataset> tmp_hash=m_datasets.get(repository_name);
            if (data_type==0)
            {
                Picture_Dataset tmp=(Picture_Dataset)m_datasets.get(repository_name).get(version_id);
                tmp.submit();
                tmp.lock_user_id="";
                tmp_hash.put(version_id,tmp);
            }
            else if (data_type==1)
            {
                Text_Dataset tmp=(Text_Dataset)m_datasets.get(repository_name).get(version_id);
                tmp.submit();
                tmp.lock_user_id="";
                tmp_hash.put(version_id,tmp);
            }
            else
            {
                Table_Dataset tmp=(Table_Dataset)m_datasets.get(repository_name).get(version_id);
                tmp.submit();
                tmp.lock_user_id="";
                tmp_hash.put(version_id,tmp);
            }
            m_datasets.put(repository_name,tmp_hash);
        }
        else if (cmd[0].equals("search"))
        {
            String search_user = cmd[1];

            String search_path = root_path + search_user + "/";
            File dir = new File(search_path);
            if (!dir.exists()) {
                return "!!!";
            }

            File[] repositories = dir.listFiles();
            int len=repositories.length;

            for (int i=0;i<len;i++)
            {
                if (!repositories[i].isDirectory())
                    continue;
                String repo_name = repositories[i].getName();
                if (cmd.length == 2 || repo_name.indexOf(cmd[2]) != -1)
                {
                    res = res + repo_name + "\n";
                }
            }
        }
        else if (cmd[0].equals("download"))        //下载数据
        {
            String user_id = cmd[1];
            String repository_name = cmd[2];
            int version_id = Integer.parseInt(cmd[3]) + 1;

            String download_path = root_path + user_id + "/" + repository_name + "/" + "version" + version_id + "/";
            File dir = new File(download_path);
            File[] all_files = dir.listFiles();
            int len=all_files.length;
            int flag = 0;
            List<File> fileList = new ArrayList<>();
            for (int i=0;i<len;i++)
            {
                String repo_name = all_files[i].getName();
                if (repo_name.indexOf("data.csv") != -1)
                {
                    flag = 0; // table
//                    res = download_path + "data.csv";
//                    return res;
                }
                if (repo_name.indexOf("pictures") != -1)
                {
                    flag = 1; // picture
                    fileList.add(new File(download_path + "pictures/"));
                    fileList.add(new File(download_path + "labels/"));
                    break;
                }
                if (repo_name.indexOf("texts") != -1)
                {
                    flag = 2; // text
                    fileList.add(new File(download_path + "texts/"));
                    fileList.add(new File(download_path + "labels/"));
                    break;
                }
            }

            File file = null;
            if (flag != 0)
            {
                FileOutputStream fos1 = new FileOutputStream(new File(download_path + "data.zip"));
                toZip(download_path, fos1);
                file = new File(download_path + "data.zip");
                res =  "download/"+user_id + "/" + repository_name + "/" + "version" + version_id + "/"+"data.zip";
            }
            else
            {
                file = new File(download_path + "data.csv");
                res =  "download/"+user_id + "/" + repository_name + "/" + "version" + version_id + "/"+"data.csv";
            }

            System.out.println("PATH: " + file.getPath());
            URI uri = file.toURI();
            System.out.println("URI: " + uri.toString());
            URL url;
            url = uri.toURL();
            System.out.println("URL: " + url.toString());
        }
        return res;
    }

    public void setUpServer(int port){
        try {
            HttpServer httpServer = HttpServer.create(new InetSocketAddress(port), 0);
            System.out.println("succeed");
            httpServer.createContext("/", new HttpHandler() {
                @Override
                public void handle(HttpExchange httpExchange) throws IOException {
                    class TestCallable implements Callable<String> {
                        public HttpExchange httpExchange;
                        public String res;
                        public TestCallable(HttpExchange httpExchange){
                            this.httpExchange=httpExchange;
                        }
                        @Override
                        public String call() throws Exception {
                            InputStreamReader isr = new InputStreamReader(httpExchange.getRequestBody(),"UTF-8");
                            BufferedReader bf = new BufferedReader(isr);
                            String results = "";
                            String newLine = "";
                            while((newLine = bf.readLine()) != null){
                                results += newLine+"\n";
                            }
                            bf.close();
                            isr.close();
                            responsetype=0;
                            String res=Parse_Command(results);
                            httpExchange.getResponseHeaders().add("Access-Control-Allow-Origin","*");
                            OutputStream os=httpExchange.getResponseBody();
                            if (responsetype==1) {
                                String s=Base64.getEncoder().encodeToString(resbuf);
                                httpExchange.sendResponseHeaders(200, s.length());
                                httpExchange.getResponseHeaders().add("Content-Type", "text/html;charset=GBK");
                                OutputStreamWriter osr = new OutputStreamWriter(os, StandardCharsets.UTF_8);
                                //System.out.println(s);
                                BufferedWriter bfw = new BufferedWriter(osr);
                                bfw.write(s);
                                bfw.close();
                                osr.close();
                            }
                            else {
                                byte[] bs = res.getBytes();
                                httpExchange.sendResponseHeaders(200, bs.length);
                                httpExchange.getResponseHeaders().add("Content-Type", "text");
                                OutputStreamWriter osr = new OutputStreamWriter(os, StandardCharsets.UTF_8);
                                //System.out.println(res);
                                BufferedWriter bfw = new BufferedWriter(osr);
                                bfw.write(res);
                                bfw.close();
                                osr.close();
                            }
                            os.close();
                            return res;
                        }
                    }
                    String res="";
                    FutureTask<String> futureTask = new FutureTask(new TestCallable(httpExchange));
                    Thread thread = new Thread(futureTask);
                    thread.start();
                    /*InputStreamReader isr = new InputStreamReader(httpExchange.getRequestBody(),"UTF-8");
                    BufferedReader bf = new BufferedReader(isr);
                    String results = "";
                    String newLine = "";
                    while((newLine = bf.readLine()) != null){
                        results += newLine+"\n";
                    }
                    bf.close();
                    isr.close();
                    responsetype=0;
                    String res=Parse_Command(results);
                    httpExchange.getResponseHeaders().add("Access-Control-Allow-Origin","*");
                    OutputStream os=httpExchange.getResponseBody();
                    if (responsetype==1) {
                        String s=Base64.getEncoder().encodeToString(resbuf);
                        httpExchange.sendResponseHeaders(200, s.length());
                        httpExchange.getResponseHeaders().add("Content-Type", "text/html;charset=GBK");
                        OutputStreamWriter osr = new OutputStreamWriter(os, StandardCharsets.UTF_8);
                        //System.out.println(s);
                        BufferedWriter bfw = new BufferedWriter(osr);
                        bfw.write(s);
                        bfw.close();
                        osr.close();
                    }
                    else {
                        byte[] bs = res.getBytes();
                        httpExchange.sendResponseHeaders(200, bs.length);
                        httpExchange.getResponseHeaders().add("Content-Type", "text");
                        OutputStreamWriter osr = new OutputStreamWriter(os, StandardCharsets.UTF_8);
                        //System.out.println(res);
                        BufferedWriter bfw = new BufferedWriter(osr);
                        bfw.write(res);
                        bfw.close();
                        osr.close();
                    }
                    os.close();*/
                }
            });
            httpServer.start();
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

        MyServer cs = new MyServer();

        cs.setUpServer(8088);

    }

}
