# Daily Data Checker

## 文件

所需的dag文件在``PySpyder/dags/workitems.daily_data_checker``目录下，所用的检查脚本在svn:

```
https://10.63.6.72/svn/lic/DataCheck/branches/new_checker/DataChecker/branches/new_checker
```

## 步骤

* 将dag文件``daily_data_checker.py``拷贝至``$ALRFLOW_HOME/dags``

* 将dag文件中下面行指向检查脚本所在位置：

    ```python
    ...
    bash_command='python path/to/datachecker/CheckerController.py',
    ...
    ```

