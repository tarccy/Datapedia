Datapedia分工情况

后端部分：
    数据集管理部分（Dataset.java,Picture_Dataset.java,Text_Dataset.java,Table_Dataset.java）:王煊宇
    用户管理部分（User.java）:王煊宇
    服务器主体（MyServer.java）:王煊宇、黄哲涛、沈康宁
        具体功能实现（命令解析）：
            登录、注册（login、register）:黄哲涛
            登出（logout）:黄哲涛、王煊宇
            创建数据仓库、数据集相关信息（create、repositories）:王煊宇
            数据增删改（add、delete）:王煊宇
            数据保存、提交、重置（submit、）:王煊宇
            数据集搜索（search）:沈康宁
            数据集后台打包：沈康宁
            数据集下载（download）:沈康宁
            数据集和暂存数据集翻页（view_dataset，view_tmp_dataset）:黄哲涛
            权限赋予：王煊宇（apply_permission）、黄哲涛（enable、view_permission、check_permission）
            多人协作锁的实现：王煊宇（lock）、曹修齐
            版本树后端实现：王煊宇、曹修齐



前端部分：
    登录、注册界面（login.html）:黄哲涛
    搜索界面（search.html）:沈康宁
    个人仓库界面（datapedia.html）黄哲涛
    数据仓库界面，版本树前端实现（data_repository.html）：黄哲涛
    图片数据集界面（picture_dataset.html）:黄哲涛、沈康宁、王煊宇（美化）
    文本数据集界面（text_dataset.html）:黄哲涛、沈康宁、王煊宇（美化）
    表格数据集界面（table_dataset.html）:黄哲涛、沈康宁、王煊宇（美化）
    css编写（style.css、picstyle.css、textstyle.css、tablestyle.css）：黄哲涛、沈康宁、王煊宇

前端测试：沈康宁

后端测试：王煊宇、黄哲涛

协调对接部分：
    Patahub:沈康宁
    Datagit:曹修齐

服务器部署以及相关调试:王煊宇


