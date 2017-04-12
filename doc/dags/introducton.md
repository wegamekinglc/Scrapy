# Introduction

``airflow``使用``python``语言作为描述计划任务的工具。每个相关的计划任务组被称为``dag``（有向无环图）。``dag``默认的放置位置：

```
AIRFLOW_HOME/dags
```

下面的部分，每一个章节都是一个dag的例子，包括：

* 定时网页的抓取