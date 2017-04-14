# Update Market Data

## 文件

所需的dag文件在``PySpyder/dags/workitems/daily_data_checker``目录下，所用的数据更新脚本在svn:

```
https://10.63.6.72/svn/Lishun/Datacenter/trunk
```

一些中间的bat文件也在``PySpyder/dags/workitems/daily_data_checker``目录下。

## 步骤

* 将dag文件``update_market_data.py``拷贝至``$ALRFLOW_HOME/dags``；

* 将所有的bat文件拷贝至``windows``主机：

    ```
    OpenSSH_INSTALL_FOLDER/home/wegamekinglc
    ```

    目录下，这里的``OpenSSH_INSTALL_FOLDER``是你的OpenSSH服务器端安装目录。

* 将bat文件中datacenter的指向，指向正确的数据更新脚本根目录；