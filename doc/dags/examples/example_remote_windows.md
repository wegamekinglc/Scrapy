# Remote Windows Call

## 预备

请先按照[windows远程](../airflow/window.md)章节的描述配置好``Linux``主机和``Windows``主机的互联。

## 文件

该例子相关的文件在``PySpyder/dags/examples``文件目录下：

```
remote_windows_call.bat
remote_windows_call.py
```

## 步骤

1. 将``remote_windows_call.bat``拷贝至``windows``主机：

    ```
    OpenSSH_INSTALL_FOLDER/home/wegamekinglc
    ```

    目录下，这里的``OpenSSH_INSTALL_FOLDER``是你的OpenSSH服务器端安装目录。

2. 在``Linux``主机上，将``remote_windows_call.py``拷贝至：

    ```
    AIRFLOW_HOME/dags
    ```

    目录下。