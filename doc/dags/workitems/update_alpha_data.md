# Update Alpha Data

## 文件

所需的dag文件在``PySpyder/dags/workitems/update_alpha_data``目录下，所用的数据更新脚本在：

```
https://10.63.6.72/svn/lishun/Research/DataUpdate
https://10.63.6.72/svn/lishun/Research/DataUpdate500
https://10.63.6.72/svn/lic/MultiFactorTradingSystem
```

一些中间的bat文件也在``PySpyder/dags/workitems/update_alpha_data``目录下。

同时该文件夹下有对应的``matlab``的脚本文件。

## 步骤

* 将dag文件``update_alpha_data.py``拷贝至``$ALRFLOW_HOME/dags``；

* 将所有的bat文件拷贝至``windows``主机：

    ```
    OpenSSH_INSTALL_FOLDER/home/wegamekinglc
    ```

    目录下，这里的``OpenSSH_INSTALL_FOLDER``是你的OpenSSH服务器端安装目录。

* 将bat文件中PATH的指向，指向正确的您本机的python以及tinysoft地址；

* 将m文件的工作目录改至您本机对应的svn地址。