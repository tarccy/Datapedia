import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class Picture_Dataset extends Dataset {
    enum picture_type {            //操作类型
        JPG, PNG;
    }

    ;

    enum operation_type {            //操作类型
        ADD, DELETE, MODIFY;
    }

    ;
    public operation_type op;       //当前操作类型
    private List<String> picture_link; //图片地址
    private List<String> label_link;   //标签地址
    private List<Integer> is_delete;   //是否被删除
    private List<String> modified_label;  //修改后的标签
    private List<Integer> is_modified; //是否被修改
    private List<byte[]> add_pics;    //增加的图片
    private List<String> add_labels; //增加的标签
    private String root_path;         //根目录

    public Picture_Dataset(String userid,String name, int id) {
        picture_link = new ArrayList<String>();
        label_link = new ArrayList<String>();
        repository_name = name;
        version_id = id;
        user_id = userid;
        root_path = "/root/datapedia/datahub/repositories/"
                + user_id+"/" +repository_name
                + "/version" + Integer.toString(version_id) + "/";
    }

    public void load(int op_type) throws IOException {
        picture_link = new ArrayList<String>();
        label_link = new ArrayList<String>();
        modified_label = new ArrayList<String>();
        File tmp=new File(root_path + "tmp_pictures/");
        if (tmp.exists()) {
            removeFolder(root_path + "tmp_pictures/");
            removeFolder(root_path + "tmp_labels/");
            removeFolder(root_path + "tmp_index.txt");
        }
        copyFolder(root_path + "pictures/", root_path + "tmp_pictures/");
        copyFolder(root_path + "labels/", root_path + "tmp_labels/");
        copyFolder(root_path + "index.txt", root_path + "tmp_index.txt");
        String pic_path = root_path + "tmp_pictures/";
        String label_path = root_path + "tmp_labels/";
        File pic_dir = new File(pic_path);
        if (!pic_dir.exists()) {
            System.out.println(pic_path + " not exists");
            return;
        }
        File[] pics = pic_dir.listFiles();
        for (int i = 0; i < pics.length; i++) {
            File fs = pics[i];
            if (fs.isFile()) {
                try {
                    picture_link.add(fs.getCanonicalPath());
                    System.out.println(fs.getCanonicalPath());
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
                op = operation_type.MODIFY;
                ready_for_modified();
                break;
        }
    }

    public void reload(int op_type) throws IOException {
        picture_link = new ArrayList<String>();
        label_link = new ArrayList<String>();
        modified_label = new ArrayList<String>();
        File tmp=new File(root_path + "tmp_pictures/");
        String pic_path = root_path + "tmp_pictures/";
        String label_path = root_path + "tmp_labels/";
        File pic_dir = new File(pic_path);
        if (!pic_dir.exists()) {
            load(op_type);
            //System.out.println(pic_path + " not exists");
            return;
        }
        File[] pics = pic_dir.listFiles();
        for (int i = 0; i < pics.length; i++) {
            File fs = pics[i];
            if (fs.isFile()) {
                try {
                    picture_link.add(fs.getCanonicalPath());
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
                op = operation_type.MODIFY;
                ready_for_modified();
                break;
        }
    }

    public void add_picture(byte[] picture, String label_value) {
        add_pics.add(picture);
        add_labels.add(label_value);
    }

    public void ready_for_add() {
        add_pics = new ArrayList<byte[]>();
        add_labels = new ArrayList<String>();
    }

    public void ready_for_delete() {
        assert (picture_link.size() == label_link.size());
        int length = picture_link.size();
        is_delete = new ArrayList<Integer>();
        for (int i = 0; i < length; i++) {
            is_delete.add(0);
        }
    }

    public void ready_for_modified() {
        assert (picture_link.size() == label_link.size());
        int length = picture_link.size();
        is_modified = new ArrayList<Integer>();
        for (int i = 0; i < length; i++) {
            is_modified.add(0);
        }
    }

    public void delete_picture(Integer id) {
        is_delete.set(id, 1);
    }

    public void modify_label(Integer id, String label_value) {
        modified_label.set(id, label_value);
        is_modified.set(id, 1);
    }
    public void save() throws IOException{
        System.out.println("Save type:"+op);
        if (op == operation_type.ADD) {
            int length = add_pics.size();
            int global_index = 0;
            String path = root_path + "tmp_index.txt";
            File fs = new File(path);
            try {
                BufferedReader in = new BufferedReader(new FileReader(fs));
                String str;
                if ((str = in.readLine()) != null) {
                    global_index = Integer.parseInt(str);
                    System.out.println(Integer.parseInt(str));
                }
                in.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            for (int i = 0; i < length; i++) {
                byte[] pic = add_pics.get(i);
                String label = add_labels.get(i);
                String pic_path = root_path + "tmp_pictures/" + String.format("%06d", global_index+i) + ".jpg";
                String label_path = root_path + "tmp_labels/" + String.format("%06d", global_index+i) + ".txt";
                try {
                    fs = new File(pic_path);
                    BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(fs));
                    bos.write(pic);
                    bos.close();
                    fs = new File(label_path);
                    BufferedWriter out = new BufferedWriter(new FileWriter(fs));
                    out.write(label);
                    out.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            try {
                fs = new File(path);
                BufferedWriter out = new BufferedWriter(new FileWriter(fs));
                out.write(Integer.toString(global_index + length));
                out.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            //reload(0);
        } else if (op == operation_type.DELETE) {
            int length = picture_link.size();
            for (int i = 0; i < length; i++) {
                if (is_delete.get(i) == 1) {
                    String pic_path = picture_link.get(i);
                    String label_path = label_link.get(i);
                    File d_pic = new File(pic_path);
                    if (d_pic.delete()) {
                        //System.out.println("Deleted the file: " + d_pic.getName());
                    } else {
                        //System.out.println("Failed to delete the file.");
                    }
                    File d_label = new File(label_path);
                    if (d_label.delete()) {
                        //System.out.println("Deleted the file: " + d_label.getName());
                    } else {
                        //System.out.println("Failed to delete the file." + d_label.getName());
                    }
                }
            }
            //reload(1);
        } else if (op == operation_type.MODIFY) {
            int length = picture_link.size();
            for (int i = 0; i < length; i++) {
                if (is_modified.get(i) == 1) {
                    String path = label_link.get(i);
                    String value = modified_label.get(i);
                    try {
                        File fs = new File(path);
                        BufferedWriter out = new BufferedWriter(new FileWriter(fs));
                        out.write(value);
                        out.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            //reload(2);
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
        String new_version_path=repository_path + "version" + Integer.toString(new_version_id) + "/";
        File new_version_dir=new File(new_version_path);
        new_version_dir.mkdir();
        File new_pic_dir=new File(new_version_path+"pictures/");
        new_pic_dir.mkdir();
        File new_label_dir=new File(new_version_path+"labels/");
        new_label_dir.mkdir();
        copyFolder(tmp_path+"tmp_pictures/",new_version_path+"pictures/");
        copyFolder(tmp_path+"tmp_labels/",new_version_path+"labels/");
        copyFolder(tmp_path+"tmp_index.txt",new_version_path+"index.txt");
        removeFolder(tmp_path+"tmp_pictures/");
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
        Picture_Dataset d = new Picture_Dataset("0","demo", 1);
        d.load(0);
        for (int i = 0; i < 2; i++)
            d.add_picture(new byte[5], "343");
        d.submit();
    }
}
