# Exchange Information Update

这个例子，是使用``airflow``设定定时任务抓取shan。

## 项目目录

在根目录下：``PySpyder/exchange``，相关的``dag``文件为：``pyspyder/dags/examples/update_exchange_db.py``

## 初始化数据库

在``mysql``中新建数据库，例如: ``exchange``。在该数据库下，运行如下的``sql``脚本新建数据表：

```
/PySpyder/exchange/schema/exchange_database.sql
```

注意，由于好买基金网数据中有很多中文字符，所以确保你的``mysql``的``charset``设置兼容中文，例如：``utf8``

## 配置dags

* 修改``PySpyder/utilities.py``中，关于``mysql``的设置行：

```python
exchange_db_settings = {'host': 'hostname',
                        'user': 'username',
                        'pwd': 'yourpassword',
                        'db': 'dbname',
                        'charset': 'utf8'}
```

* 拷贝``update_exchange_db.py``至``AIRFLOW_HOME/dags``文件夹下；

* 修改``update_exchange.py``中下面的行，使其指向``PySpyder``的上级目录：

```python
sys.path.append('/path/toyour/scrapy/root')
```