import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class User {
    public String userid;
    class Dataset_info{
        public String repository_name;
        public String user_id;
        public int version_id;
    }
    public List<Dataset_info> using_datasets=new ArrayList<>();
    public User(String m_userid){
        userid=m_userid;
        using_datasets=new ArrayList<>();
    }
    public void use_datasets(Dataset m_dataset){
        Dataset_info tmp=new Dataset_info();
        tmp.repository_name=m_dataset.repository_name;
        tmp.user_id=m_dataset.user_id;
        tmp.version_id=m_dataset.version_id;
        using_datasets.add(tmp);
    }
    public void exit_datasets(Dataset m_dataset){
        int len=using_datasets.size();
        int delete_index=0;
        for (int i=0;i<len;i++)
        {
            Dataset_info tmp=using_datasets.get(i);
            if (tmp.version_id==m_dataset.version_id
                    && Objects.equals(tmp.repository_name, m_dataset.repository_name)
                    && Objects.equals(tmp.user_id, m_dataset.user_id))
            {
                delete_index=i;
                break;
            }
        }
        using_datasets.remove(delete_index);
    }

}
