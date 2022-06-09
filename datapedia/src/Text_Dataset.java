import java.io.*;
import java.util.ArrayList;
import java.util.List;
public class Text_Dataset extends Dataset{
    enum operation_type {            //操作类型
        ADD,DELETE,MODIFY_TEXT,MODIFY_LABEL;
    };
    public operation_type op;       //当前操作类型
    private List<String> text_link; //图片地址
    private List<String> label_link;   //标签地址
    private List<Integer> is_delete;   //是否被删除
    private List<String> modified_text;  //修改后的标签
    private List<String> modified_label;  //修改后的标签
    private List<Integer> is_text_modified; //是否被修改
    private List<Integer> is_label_modified; //是否被修改
    private List<String> add_texts;    //增加的图片
    private List<String> add_labels; //增加的标签
    private String root_path;         //根目录
    public Text_Dataset(String userid,String name,int id){
        text_link=new ArrayList<String>();
        label_link=new ArrayList<String>();
        repository_name=name;
        version_id=id;
        user_id=userid;
        root_path="/root/datapedia/datahub/repositories/"
                + user_id+"/" +repository_name
                +"/version"+Integer.toString(version_id)+"/";
    }
    public void load(int op_type) throws IOException {
        text_link = new ArrayList<String>();
        label_link = new ArrayList<String>();
        modified_label = new ArrayList<String>();
        File tmp=new File(root_path + "tmp_texts/");
        if (tmp.exists()) {
            removeFolder(root_path + "tmp_texts/");
            removeFolder(root_path + "tmp_labels/");
            removeFolder(root_path + "tmp_index.txt");
        }
        copyFolder(root_path + "texts/", root_path + "tmp_texts/");
        copyFolder(root_path + "labels/", root_path + "tmp_labels/");
        copyFolder(root_path + "index.txt", root_path + "tmp_index.txt");
        String text_path = root_path + "tmp_texts/";
        String label_path = root_path + "tmp_labels/";
        File text_dir = new File(text_path);
        if (!text_dir.exists()) {
            System.out.println(text_path + " not exists");
            return;
        }
        File texts[] = text_dir.listFiles();
        for (int i = 0; i < texts.length; i++) {
            File fs = texts[i];
            if (fs.isFile()) {
                try {
                    text_link.add(fs.getCanonicalPath());
                    //System.out.println(fs.getCanonicalPath());
                } catch (IOException e) {
                    System.out.println("Wrong!");
                }
            }
        }
        File label_dir = new File(label_path);
        if (!label_dir.exists()) {
            System.out.println(label_path + " not exists");
            return;
        }
        File labels[] = label_dir.listFiles();
        for (int i = 0; i < labels.length; i++) {
            File fs = labels[i];
            if (fs.isFile()) {
                try {
                    label_link.add(fs.getCanonicalPath());
                    System.out.println(fs.getCanonicalPath());
                    /*BufferedReader in = new BufferedReader(new FileReader(fs));
                    String str;
                    if ((str = in.readLine()) != null) {
                        modified_label.add(str);
                        //System.out.println(Integer.parseInt(str));
                    }
                    in.close();*/
                    modified_label.add("");
                } catch (IOException e) {
                    System.out.println("Wrong!");
                }
            }
        }
        switch (op_type) {
            case 0:
                op = operation_type.ADD;
                ready_for_add();
                break;
            case 1:
                op = operation_type.DELETE;
                ready_for_delete();
                break;
            case 2:
                op = operation_type.MODIFY_TEXT;
                ready_for_modified();
                break;
            case 3:
                op = operation_type.MODIFY_LABEL;
                ready_for_modified();
                break;
        }
    }
    public void reload(int op_type) throws IOException {
        text_link = new ArrayList<String>();
        label_link = new ArrayList<String>();
        modified_label = new ArrayList<String>();
        File tmp=new File(root_path + "tmp_texts/");
        String text_path = root_path + "tmp_texts/";
        String label_path = root_path + "tmp_labels/";
        File text_dir = new File(text_path);
        if (!text_dir.exists()) {
            load(op_type);
            System.out.println(text_path + " not exists");
            return;
        }
        File texts[] = text_dir.listFiles();
        for (int i = 0; i < texts.length; i++) {
            File fs = texts[i];
            if (fs.isFile()) {
                try {
                    text_link.add(fs.getCanonicalPath());
                    //System.out.println(fs.getCanonicalPath());
                } catch (IOException e) {
                    System.out.println("Wrong!");
                }
            }
        }
        File label_dir = new File(label_path);
        if (!label_dir.exists()) {
            System.out.println(label_path + " not exists");
            return;
        }
        File labels[] = label_dir.listFiles();
        for (int i = 0; i < labels.length; i++) {
            File fs = labels[i];
            if (fs.isFile()) {
                try {
                    label_link.add(fs.getCanonicalPath());
                    //System.out.println(fs.getCanonicalPath());
                    /*BufferedReader in = new BufferedReader(new FileReader(fs));
                    String str;
                    if ((str = in.readLine()) != null) {
                        modified_label.add(str);
                        //System.out.println(Integer.parseInt(str));
                    }
                    in.close();*/
                    modified_label.add("");
                } catch (IOException e) {
                    System.out.println("Wrong!");
                }
            }
        }
        switch (op_type) {
            case 0:
                op = operation_type.ADD;
                ready_for_add();
                break;
            case 1:
                op = operation_type.DELETE;
                ready_for_delete();
                break;
            case 2:
                op = operation_type.MODIFY_TEXT;
                ready_for_modified();
                break;
            case 3:
                op = operation_type.MODIFY_LABEL;
                ready_for_modified();
                break;
        }
    }
    public void ready_for_add(){
        add_texts=new ArrayList<String>();
        add_labels=new ArrayList<String>();
    }
    public void ready_for_delete(){
        assert(text_link.size()==label_link.size());
        int length=text_link.size();
        is_delete=new ArrayList<Integer>();
        for (int i=0;i<length;i++)
        {
            is_delete.add(0);
        }
    }
    public void ready_for_modified(){
        assert(text_link.size()==label_link.size());
        int length=text_link.size();
        is_text_modified=new ArrayList<Integer>();
        is_label_modified=new ArrayList<Integer>();
        modified_text=new ArrayList<String>();
        for (int i=0;i<length;i++)
        {
            is_text_modified.add(0);
            is_label_modified.add(0);
            modified_label.add("0");
            modified_text.add("");
        }
    }
    public void add_text(String text,String label_value){
        add_texts.add(text);
        add_labels.add(label_value);
    }
    public void delete_text(Integer id){
        is_delete.set(id, 1);
    }
    public void modify_text(Integer id,String text){
        modified_text.set(id, text);
        is_text_modified.set(id,1);
    }
    public void modify_label(Integer id,String label_value){
        modified_label.set(id, label_value);
        is_label_modified.set(id,1);
    }
    public void save() throws IOException {
        System.out.println("op type:"+op);
        if (op==operation_type.ADD)
        {
            int length=add_texts.size();
            int global_index=0;
            String path=root_path+"tmp_index.txt";
            File fs=new File(path);
            try {
                BufferedReader in = new BufferedReader(new FileReader(fs));
                String str;
                if ((str = in.readLine()) != null) {
                    global_index=Integer.parseInt(str);
                    System.out.println(Integer.parseInt(str));
                }
                in.close();
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
            for (int i=0;i<length;i++)
            {
                String text=add_texts.get(i);
                String label=add_labels.get(i);
                String text_path=root_path+"tmp_texts/"+String.format("%06d", global_index+i)+".txt";
                String label_path=root_path+"tmp_labels/"+String.format("%06d", global_index+i)+".txt";
                try {
                    fs=new File(text_path);
                    BufferedWriter out = new BufferedWriter(new FileWriter(fs));
                    System.out.println(text);
                    out.write(text);
                    out.close();
                    fs = new File(label_path);
                    out = new BufferedWriter(new FileWriter(fs));
                    out.write(label);
                    out.close();
                }
                catch(Exception e){
                    e.printStackTrace();
                }
            }
            try {
                fs=new File(path);
                BufferedWriter out = new BufferedWriter(new FileWriter(fs));
                out.write(Integer.toString(global_index+length));
                out.close();
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
            //reload(0);
        }
        else if (op==operation_type.DELETE)
        {
            int length=text_link.size();
            for (int i=0;i<length;i++)
            {
                if (is_delete.get(i)==1)
                {
                    String text_path=text_link.get(i);
                    String label_path=label_link.get(i);
                    File d_text = new File(text_path);
                    if (d_text.delete()) {
                        //System.out.println("Deleted the file: " + d_text.getName());
                    } else {
                        //System.out.println("Failed to delete the file.");
                    }
                    File d_label = new File(label_path);
                    if (d_label.delete()) {
                        //System.out.println("Deleted the file: " + d_label.getName());
                    } else {
                        //System.out.println("Failed to delete the file."+ d_label.getName());
                    }
                }
            }
            //reload(1);
        }
        else if (op==operation_type.MODIFY_TEXT)
        {
            int length=text_link.size();
            for (int i=0;i<length;i++)
            {
                System.out.println("is_text_modified:"+is_text_modified);
                if (is_text_modified.get(i)==1)
                {
                    String path=text_link.get(i);
                    String text=modified_text.get(i);
                    try {
                        File fs = new File(path);
                        BufferedWriter out = new BufferedWriter(new FileWriter(fs));
                        System.out.println(text);
                        out.write(text);
                        out.close();
                    }
                    catch(Exception e){
                        e.printStackTrace();
                    }
                }
            }
            //reload(2);
        }
        else if (op==operation_type.MODIFY_LABEL)
        {
            int length=text_link.size();
            for (int i=0;i<length;i++)
            {
                if (is_label_modified.get(i)==1)
                {
                    String path=label_link.get(i);
                    String value=modified_label.get(i);
                    try {
                        File fs = new File(path);
                        BufferedWriter out = new BufferedWriter(new FileWriter(fs));
                        out.write(value);
                        out.close();
                    }
                    catch(Exception e){
                        e.printStackTrace();
                    }
                }
            }
            //reload(3);
        }
    }

    public void submit() throws IOException {
        save();
        String repository_path="/root/datapedia/datahub/repositories/" + user_id+"/"+repository_name+"/";
        String tmp_path=repository_path + "version" + Integer.toString(version_id) + "/";
        int new_version_id;
        File version_dir = new File(repository_path);
        File[] versions = version_dir.listFiles();
        new_version_id=versions.length-3;
        String new_version_path=repository_path + "version" + Integer.toString(new_version_id)+"/";
        System.out.println(new_version_path);
        File new_version_dir=new File(new_version_path);
        new_version_dir.mkdir();
        File new_text_dir=new File(new_version_path+"texts/");
        new_text_dir.mkdir();
        File new_label_dir=new File(new_version_path+"labels/");
        new_label_dir.mkdir();
        copyFolder(tmp_path+"tmp_texts/",new_version_path+"texts/");
        copyFolder(tmp_path+"tmp_labels/",new_version_path+"labels/");
        copyFolder(tmp_path+"tmp_index.txt",new_version_path+"index.txt");
        removeFolder(tmp_path+"tmp_texts/");
        removeFolder(tmp_path+"tmp_labels/");
        removeFolder(tmp_path+"tmp_index.txt");
        File version_tree=new File(repository_path+"version_tree.txt");
        try {
            BufferedWriter out=new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(repository_path+"version_tree.txt",true)));
            String new_info="{ id:"+Integer.toString(new_version_id)+", pId:"
                    +Integer.toString(version_id)+", name:\"版本"+Integer.toString(new_version_id)+"\"}\n";
            out.write(new_info);
            out.close();
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        Text_Dataset d=new Text_Dataset(Integer.toString(0),"demo1",1);
        d.load(2);
        d.modify_text(2,"奈斯");
        d.save();
    }
}
