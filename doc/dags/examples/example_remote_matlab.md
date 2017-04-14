# Remote Matlab Call

使用``airflow``远程方式运行``matlab``的方法与[]

## 预备

* 请先按照[windows远程](../airflow/window.md)章节的描述配置好``Linux``主机和``Windows``主机的互联。

* 在``windows``主机上安装``matlab``

## 文件

该例子相关的文件在``PySpyder/dags/examples``文件目录下：

```
remote_matlab_script.bat
remote_matlab_script.m
remote_matlab_script.py
```

## 步骤

1. 将``remote_matlab_script.bat``以及``remote_matlab_script.m``拷贝至``windows``主机：

    ```
    OpenSSH_INSTALL_FOLDER/home/wegamekinglc
    ```

    目录下，这里的``OpenSSH_INSTALL_FOLDER``是你的OpenSSH服务器端安装目录。

2. 在``Linux``主机上，将``remote_matlab_script.py``拷贝至：

    ```
    AIRFLOW_HOME/dags
    ```

    目录下。