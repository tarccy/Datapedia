import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class Dataset {
    private int download_times;
    protected String repository_name;  //仓库名
    protected int version_id;          //版本号
    protected String user_id;
    protected String lock_user_id="";
    protected long lock_time;
    public Dataset(){
        version_id=0;
        download_times=0;
    }
    public Dataset(int id){
        version_id=id;
        download_times=0;
    }
    public boolean matched(String name,int id)
    {
        if (repository_name.equals(name)&&version_id==id)
            return true;
        else
            return false;
    }
    public void download(){

    }
    public void update_download_times(){
        download_times++;
    }
    public void copyFolder(String olderFolder, String newFolder) throws IOException {

        File file = new File(newFolder);

        File oldFile = new File(olderFolder);

        if (!file.exists()) {
            if (!oldFile.isFile())
                file.mkdir();
            else {
                file.createNewFile();
                FileInputStream in = new FileInputStream(oldFile);
                FileOutputStream out = new FileOutputStream(file);
                byte[] bt = new byte[1024];
                int len = in.read(bt);
                while (len != -1) {
                    out.write(bt, 0, len);
                    len = in.read(bt);
                }
                out.flush();
                out.close();
                in.close();
                return;
            }
        }

        String[] files = oldFile.list();// 获得原文件的文件列表

        File tempFile = null;

        int num_file = files.length;

        for (int i = 0; i < num_file; i++) {

            if (olderFolder.endsWith(File.separator)) {

                tempFile = new File(olderFolder + files[i]);

            } else {

                tempFile = new File(olderFolder + File.separator + files[i]);

            }

            if (tempFile.isFile()) {// 临时文件对象时文件

                FileInputStream in = new FileInputStream(tempFile);

                FileOutputStream out = new FileOutputStream(newFolder + "/" + (tempFile.getName().toString()));

                byte[] bt = new byte[1024];

                int len = in.read(bt);

                while (len != -1) {

                    out.write(bt, 0, len);

                    len = in.read(bt);

                }

                out.flush();

                out.close();

                in.close();

            }

            if (tempFile.isDirectory()) {
                copyFolder(olderFolder + "/" + files[i], newFolder + "/" + files[i]);//递归调用
            }

        }

    }
    public void removeFolder(String Folder) throws IOException {

        File file = new File(Folder);

        if (file.isFile()) {
            file.delete();
            return;
        }

        String[] files = file.list();// 获得原文件的文件列表

        File tempFile = null;

        int num_file = files.length;

        for (int i = 0; i < num_file; i++) {

            if (Folder.endsWith(File.separator)) {

                tempFile = new File(Folder + files[i]);

            } else {

                tempFile = new File(Folder + File.separator + files[i]);

            }

            if (tempFile.isFile()) {// 临时文件对象时文件
                tempFile.delete();
            }

            if (tempFile.isDirectory()) {
                removeFolder(Folder + "/" + files[i]);//递归调用
                tempFile.delete();
            }

        }
        file.delete();
    }
}
