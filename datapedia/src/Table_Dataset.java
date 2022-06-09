import java.io.*;
import java.util.ArrayList;
import java.util.List;
public class Table_Dataset extends Dataset{
    enum operation_type {            //操作类型
        ADD,DELETE,MODIFY;
    };
    private int columns=0;
    private String attr="";
    public operation_type op;       //当前操作类型
    private List<Integer> is_delete;   //是否被删除
    private List<String> modified_row;  //修改后的标签
    private List<String> add_rows;    //增加的图片
    private String root_path;         //根目录
    public Table_Dataset(String userid,String name,int id){
        repository_name=name;
        version_id=id;
        user_id=userid;
        root_path="/root/datapedia/datahub/repositories/"
                + user_id+"/" +repository_name
                +"/version"+Integer.toString(version_id)+"/";
    }
    public void load(int op_type) throws IOException {
        modified_row=new ArrayList<String>();
        File tmp=new File(root_path + "tmp_data.csv");
        if (tmp.exists()) {
            removeFolder(root_path + "tmp_data.csv");
        }
        copyFolder(root_path + "data.csv", root_path + "tmp_data.csv");
        String table_path=root_path+"tmp_data.csv";
        File table = new File(table_path);
        if(!table.exists()){
            System.out.println(table + " not exists");
            return;
        }
        try {
            BufferedReader in = new BufferedReader(new FileReader(table));
            String str;
            if ((str = in.readLine()) != null) {
                attr=str;
                String[] attributes=str.split(",");
                columns=attributes.length;
                System.out.println(Integer.toString(columns));
            }
            while((str = in.readLine()) != null){
                modified_row.add(str);
                System.out.println(str);
            }
            in.close();
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        switch (op_type) {
            case 0:
                op=operation_type.ADD;
                ready_for_add();
                break;
            case 1:
                op=operation_type.DELETE;
                ready_for_delete();
                break;
            case 2:
                op=operation_type.MODIFY;
                ready_for_modified();
                break;
        }
    }
    public void reload(int op_type) throws IOException {
        modified_row=new ArrayList<String>();
        File tmp=new File(root_path + "tmp_data.csv");
        String table_path=root_path+"tmp_data.csv";
        File table = new File(table_path);
        if(!table.exists()){
            load(op_type);
            System.out.println(table + " not exists");
            return;
        }
        try {
            BufferedReader in = new BufferedReader(new FileReader(table));
            String str;
            if ((str = in.readLine()) != null) {
                attr=str;
                String[] attributes=str.split(",");
                columns=attributes.length;
                //System.out.println(Integer.toString(columns));
            }
            while((str = in.readLine()) != null){
                modified_row.add(str);
                //System.out.println(str);
            }
            in.close();
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        switch (op_type) {
            case 0:
                op=operation_type.ADD;
                ready_for_add();
                break;
            case 1:
                op=operation_type.DELETE;
                ready_for_delete();
                break;
            case 2:
                op=operation_type.MODIFY;
                ready_for_modified();
                break;
        }
    }
    public void ready_for_add(){
        add_rows=new ArrayList<String>();
    }
    public void ready_for_delete(){
        int length=modified_row.size();
        is_delete=new ArrayList<Integer>();
        for (int i=0;i<length;i++)
        {
            is_delete.add(0);
        }
    }
    public void ready_for_modified(){
        int length=modified_row.size();
    }
    public void add_row(String[] row){
        int length=row.length;
        assert(length>=2);
        assert(length==columns);
        assert(row[columns-1]=="label");
        String temp_row="";
        for (int i=0;i<length-1;i++)
        {
            temp_row=temp_row+row[i]+",";
        }
        temp_row=temp_row+row[length-1];
        add_rows.add(temp_row);
    }
    public void delete_row(Integer id){
        is_delete.set(id, 1);
    }
    public void modify_row(Integer id,String[] row){
        int length=row.length;
        assert(length>=2);
        assert(length==columns);
        assert(row[columns-1]=="label");
        String temp_row="";
        for (int i=0;i<length-1;i++)
        {
            temp_row=temp_row+row[i]+",";
        }
        temp_row=temp_row+row[length-1];
        modified_row.set(id,temp_row);
    }
    public void save() throws IOException{
        if (op==operation_type.ADD)
        {
            int length=modified_row.size();
            String table_path=root_path+"tmp_data.csv";
            File fs=new File(table_path);
            try {
                BufferedWriter out = new BufferedWriter(new FileWriter(fs));
                out.write(attr+"\n");
                for (int i=0;i<length;i++)
                {
                    String row=modified_row.get(i);
                    out.write(row+"\n");
                }
                length=add_rows.size();
                for (int i=0;i<length;i++)
                {
                    String row=add_rows.get(i);
                    out.write(row+"\n");
                }
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
            int length=modified_row.size();
            String table_path=root_path+"tmp_data.csv";
            File fs=new File(table_path);
            try {
                BufferedWriter out = new BufferedWriter(new FileWriter(fs));
                out.write(attr+"\n");
                for (int i=0;i<length;i++)
                {
                    if (is_delete.get(i)==0)
                    {
                        String row=modified_row.get(i);
                        out.write(row+"\n");
                    }
                }
                out.close();
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
            //reload(1);
        }
        else if (op==operation_type.MODIFY)
        {
            int length=modified_row.size();
            String table_path=root_path+"tmp_data.csv";
            File fs=new File(table_path);
            try {
                BufferedWriter out = new BufferedWriter(new FileWriter(fs));
                out.write(attr+"\n");
                for (int i=0;i<length;i++) {
                    String row = modified_row.get(i);
                    out.write(row + "\n");
                }
                out.close();
            }
            catch(Exception e)
            {
                e.printStackTrace();
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
        copyFolder(tmp_path+"tmp_data.csv",new_version_path+"data.csv");
        removeFolder(tmp_path+"tmp_data.csv");
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
        Table_Dataset d=new Table_Dataset("0","demo2",1);
        d.load(2);
        String row[]=new String[]{"Brazer","56","1"};
        d.modify_row(1,row);
        d.submit();
    }
}
