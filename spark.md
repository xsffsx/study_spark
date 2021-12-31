# spark内存计算框架

## 一、课前准备

* 1、安装好对应版本的hadoop集群

* 2、安装好对应版本的zookeeper集群

## 二、课堂主题

本堂课主要围绕 ==spark的基础知识点== 进行讲解。主要包括以下几个方面

1. spark引入及特性
2. spark内置组件
3. spark核心概念
4. spark运行模式及spark-shell和spark-submit的使用
5. 通过IDEA开发spark程序
6. RDD引入

## 三、课堂目标

* 1、掌握spark的集群架构
* 2、掌握spark集群的安装部署
* 3、掌握spark-shell的使用
* 4、掌握通过IDEA开发spark程序

## 四、知识要点

### 4.1 Spark是什么

![image-20200910163209076](spark.assets/image-20200910163209076.png)

- **Apache Spark™** is a unified analytics engine for large-scale data processing.
- **Apache Spark™**是用于大规模数据处理的统一分析引擎

>Spark是一个快速（基于内存），通用、可扩展的计算引擎，采用Scala语言编写。2009年诞生于UC Berkeley(加州大学伯克利分校，CAL的AMP实验室)，2010年开源，2013年6月进入Apach孵化器，同年由美国伯克利大学 AMP 实验室的 Spark 大数据处理系统多位创始人联合创立Databricks（属于 Spark 的商业化公司-业界称之为数砖-数据展现-砌墙-侧面应正其不是基石，只是数据计算），2014年成为Apach顶级项目，自2009年以来，已有1200多家开发商为Spark出力！ 
>
>Spark支持Java、Scala、Python、R、SQL语言，并提供了几十种(目前80+种)高性能的算法，这些如果让我们自己来做，几乎不可能。
>
>Spark得到众多公司支持，如：阿里、腾讯、京东、携程、百度、优酷、土豆、IBM、Cloudera、Hortonworks等。
>
>spark是在Hadoop基础上的改进，是UC Berkeley AMP lab所开源的类Hadoop MapReduce的通用的并行计算框架，Spark基于map reduce算法实现的分布式计算，拥有Hadoop MapReduce所具有的优点；但不同于MapReduce的是Job中间输出和结果可以保存在内存中，从而不再需要读写HDFS，因此Spark能更好地适用于数据挖掘与机器学习等需要迭代的map reduce的算法。
>
>spark是基于内存计算框架，计算速度非常之快，但是它仅仅只是涉及到计算，并没有涉及到数据的存储，后期需要使用spark对接外部的数据源，比如hdfs。
>
>
>
>这里有个疑问？内存不够怎么办呢？
>
>- 官网[spark.apache.org](http://spark.apache.org/)文档常见问题
>
>Does my data need to fit in memory to use Spark?   我的数据需要放入内存才能使用Spark吗？
>
>No. Spark's operators spill data to disk if it does not  in memory, allowing it to run well on any sized data. Likewise, cached datasets that do not fit in memory are either spilled to disk or recomputed on the fly when needed, as determined by the RDD's [storage level](http://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-persistence).
>
>不会。如果不合适存储在内存，那么Spark的操作员会将数据溢写到磁盘上，这样就可以不管数据大小如何而良好运行。同样，不适合内存的缓存数据集要么溢出到磁盘，要么在需要时实时重新计算，这由RDD的存储级别决定。

### 4.2 四大特性

#### 4.2.1 速度快

![image-20200910170232304](spark.assets/image-20200910170232304.png)

- 运行速度提高100倍(针对hadoop2.x比较，而hadoop3.x号称比spark又快10倍)
  
- Apache Spark使用最先进的DAG调度程序，查询优化程序和物理执行引擎，实现批量和流式数据的高性能。
  
- spark比mapreduce快的2个主要原因

  1. ==基于内存==

  >（1）mapreduce任务后期再计算的时候，每一个job的输出结果会落地到磁盘，后续有其他的job需要依赖于前面job的输出结果，这个时候就需要进行大量的磁盘io操作。性能就比较低。
  >
  >
  >（2）spark任务后期再计算的时候，job的输出结果可以保存在内存中，后续有其他的job需要依赖于前面job的输出结果，这个时候就直接从内存中获取得到，避免了磁盘io操作，性能比较高
  >
  >job1---->job2----->job3----->job4----->job5----->job6
  >
  >对于spark程序和mapreduce程序都会产生shuffle阶段，在shuffle阶段中它们产生的数据都会落地到磁盘。

  2. ==进程与线程==

  >（1）mapreduce任务以进程的方式运行在yarn集群中，比如程序中有100个MapTask，一个task就需要一个进程，这些task要运行就需要开启100个进程。
  >
  >（2）spark任务以线程的方式运行在进程中，比如程序中有100个MapTask，后期一个task就对应一个线程，这里就不在是进程，这些task需要运行，这里可以极端一点：
  >只需要开启1个进程，在这个进程中启动100个线程就可以了。
  >进程中可以启动很多个线程，而开启一个进程与开启一个线程需要的时间和调度代价是不一样。 开启一个进程需要的时间远远大于开启一个线程。

#### 4.2.2 易用性

![image-20200910170744314](spark.assets/image-20200910170744314.png)

- 可以快速去编写spark程序通过 java/scala/python/R/SQL等不同语言

#### 4.2.3 通用性

![image-20200910170758202](spark.assets/image-20200910170758202.png)

> spark框架不在是一个简单的框架，可以把spark理解成一个**生态系统**，它内部是包含了很多模块，基于不同的应用场景可以选择对应的模块去使用

* ==**sparksql**==
  * 通过sql去开发spark程序做一些离线分析
* ==**sparkStreaming**==
  * 主要是用来解决公司有实时计算的这种场景
* ==**Mlib**==
  * 它封装了一些机器学习的算法库
* ==**Graphx**==
  * 图计算

#### 4.2.4 兼容性

![image-20200910170814427](spark.assets/image-20200910170814427.png)

> Spark运行在Hadoop、ApacheMesos、Kubernetes、单机版或云中。它可以访问不同的数据源

* **standAlone**
  * 它是spark自带的独立运行模式，整个任务的资源分配由spark集群的老大Master负责
* **yarn**
  * 可以把spark程序提交到yarn中运行，整个任务的资源分配由yarn中的老大ResourceManager负责
* **mesos**
  * 它也是apache开源的一个类似于yarn的资源调度平台

- **cassandra**
  - *Cassandra*是一套开源分布式NoSQL数据库系统
- **kubernetes**
  - K8s用于管理云平台中多个主机上的容器化的应用

- **hbase**
  - *HBase*是一个分布式的、面向列的开源数据库

### 4.3 内置组件

> 机器学习（MLlib），图计算（GraphicX），实时处理（SparkStreaming），SQL解析（SparkSql）

![image-20200910173948669](spark.assets/image-20200910173948669.png)

#### 4.3.1 集群资源管理

1. Spark设计为可以高效的在一个计算节点到数千个计算节点之间伸缩计算，为了实现这样的要求，同时获得最大灵活性，Spark支持在各种集群资源管理器上运行，目前支持的3种如下：（上图中下三个）

   1. Hadoop YARN（国内几乎都用）
      - 可以把spark程序提交到yarn中运行，整个任务的资源分配由yarn中的老大ResourceManager负责
   2. Apach Mesos（国外使用较多）
      - 它也是apache开源的一个类似于yarn的资源调度平台。
   3. Standalone（Spark自带的资源调度器，需要在集群中的每台节点上配置Spark）
      - 它是spark自带的集群模式，整个任务的资源分配由spark集群的老大Master负责

#### 4.3.2 Spark Core(核心库)

> 实现了Spark的基本功能，包含任务调度、内存管理、错误恢复、与存储系统交互等模块。其中还包含了对弹性分布式数据集（RDD：Resilient Distributed DataSet）的API定义

#### 4.3.3 Spark SQL(SQL解析)

> 是Spark用来操作结构化数据的程序包，通过Spark SQL 我们可以使用SQL或者HQL来查询数据。且支持多种数据源：Hive、Parquet、Json等

#### 4.3.4 Spark Streaming(实时处理)

> 是Spark提供的对实时数据进行流式计算的组件

#### 4.3.5 Spark MLlib(机器学习)

> 提供常见的机器学习功能和程序库，包括分类、回归、聚类、协同过滤等。还提供了模型评估、数据导入等额外的支持功能。

#### 4.3.6 Spark GraphX(图计算)

> 用于图形和图形并行计算的API

### 4.4 运行模式

#### 4.4.1 集群架构

- 执行架构图

![image-20200910175019398](spark.assets/image-20200910175019398.png)

- 执行流程图

![image-20200910181516788](spark.assets/image-20200910181516788.png)

- 应用结构图

![image-20200910182526828](spark.assets/image-20200910182526828.png)

#### 4.4.1 核心概念介绍

- Master
  - <font color=red>Spark特有的资源调度系统Leader，掌控整个集群资源信息，类似于Yarn框架中的ResourceManager</font>
  - 监听Worker，看Worker是否正常工作
  - Master对Worker、Application等的管理（接收Worker的注册并管理所有的Worker，接收Client提交的Application，调度等待Application并向Worker提交）

- Worker
  - Spark特有的资源调度Slave，有多个，每个Slave掌管着所有节点的资源信息，类似Yarn框架中的NodeManager
  - 通过RegisterWorker注册到Master
  - 定时发送心跳给Master
  - <font color=red>根据Master发送的Application配置进程环境，并启动ExecutorBackend（执行Task所需的计算任务进程进程）</font>
- Driver
  - Spark的驱动器，<font color=red>是执行开发程序中的main方法的线程</font>
  - 负责开发人员编写SparkContext、RDD，以及进行RDD操作的代码执行，如果使用Spark Shell，那么启动时后台自启动了一个Spark驱动器，预加载一个叫做sc的SparkContext对象(<font color=red>该对象是所有spark程序的执行入口</font>)，如果驱动器终止，那么Spark应用也就结束了。
  - 4大主要职责：
    - 将用户程序转化为作业（Job）
    - 在Executor之间调度任务（Task）
    - 跟踪Executor的执行情况
    - 通过UI展示查询运行情况
- Excutor
  - Spark Executor是一个工作节点，负责在Spark作业中运行任务，任务间相互独立。Spark应用启动时，Executor节点被同时启动，并且始终伴随着整个Spark应用的生命周期而存在，如果有Executor节点发生了故障或崩溃，Spark应用也可以继续执行，会将出错节点上的任务调度到其他Executor节点上继续运行
  - <font color=red>两个核心功能：</font>
    - 负责运行组成Spark应用的任务，并将结果返回给驱动器（Driver）
    - 它通过自身块管理器（BlockManager）为用户程序中要求缓存的RDD提供内存式存储。RDD是直接存在Executor进程内的，因此任务可以在运行时充分利用缓存数据加速运算。
- RDDs
  - Resilient Distributed DataSet：弹性分布式数据集
  - 一旦拥有SparkContext对象，就可以用它来创建RDD

- Application应用
  - <font color=red>一个SparkContext就是一个Application（一个spark的应用程序）</font>，它是包含了客户端的代码和任务运行的资源信息
- Job作业：
  - 一个行动算子(Action)就是一个Job
- Stage阶段：
  - 一次宽依赖（一次shuffle）就是一个Stage，划分是从后往前划分
- Task任务：
  - spark任务是以task线程的方式运行在worker节点对应的executor进程中
  - 一个核心就是一个Task，体现任务的并行度

#### 4.4.2 spark集群安装部署

* 事先搭建好zookeeper和hadoop集群

1、下载安装包

- https://archive.apache.org/dist/spark/spark-2.3.3/spark-2.3.3-bin-hadoop2.7.tgz
- spark-2.3.3-bin-hadoop2.7.tgz

2、规划安装目录( /kkb/install )--->注意分发之前的操作全部在主节点执行即可

3、上传安装包到服务器node01的 /kkb/soft 路径下

4、解压安装包到指定的安装目录

- tar -zxvf  spark-2.3.3-bin-hadoop2.7.tgz  -C /kkb/install

5、修改配置文件

- 进入到spark的安装目录下对应的conf文件夹，node01执行以下命令修改配置文件

- 修改spark-env.sh

  - cd /kkb/install/spark-2.3.3-bin-hadoop2.7/conf/

  - cp  spark-env.sh.template spark-env.sh

  - vim spark-env.sh 

  ```shell
    #配置java的环境变量
  export JAVA_HOME=/kkb/install/jdk1.8.0_141
  export SPARK_HISTORY_OPTS="-Dspark.history.ui.port=4000 -Dspark.history.retainedApplications=3 -Dspark.history.fs.logDirectory=hdfs://node01:8020/spark_log"
    #配置zk相关信息
  export SPARK_DAEMON_JAVA_OPTS="-Dspark.deploy.recoveryMode=ZOOKEEPER  -Dspark.deploy.zookeeper.url=node01:2181,node02:2181,node03:2181  -Dspark.deploy.zookeeper.dir=/spark"
    
  ```


- 修改slaves配置文件
  - cp  slaves.template slaves

  - vim slaves 

```shell
#指定spark集群的worker节点
node01
node02
node03
```

- node01执行以下命令修改spark-defaults.conf配置选项

```shell
cd /kkb/install/spark-2.3.3-bin-hadoop2.7/conf/
cp spark-defaults.conf.template spark-defaults.conf

vim spark-defaults.conf

spark.eventLog.enabled  true
spark.eventLog.dir       hdfs://node01:8020/spark_log
spark.eventLog.compress true
```

6、分发安装目录到其他机器

- node01执行以下命令分发安装包

```shell
cd /kkb/install/
scp -r spark-2.3.3-bin-hadoop2.7/ node02:$PWD
scp -r spark-2.3.3-bin-hadoop2.7/ node03:$PWD
```

7、hdfs创建spark日志文件夹

```
hdfs  dfs -mkdir -p /spark_log
```

#### 4.4.3 spark集群的启动和停止以及任务提交

##### 4.4..3.1 启动

1. 先启动zk
2. 启动spark集群

- 可以在任意一台服务器来执行（条件：需要任意2台机器之间实现ssh免密登录）
  - == $SPARK_HOME/sbin/start-all.sh ==
  - == sbin/start-history-server.sh ==
  - 在哪里启动这个脚本，就在当前该机器启动一个Master进程
  - 整个集群的worker进程的启动由slaves文件
- 后期可以在其他机器单独在启动master
  - == $SPARK_HOME/sbin/start-master.sh ==

##### 4.4.3.2 停止

* 在处于active Master主节点执行
  * == $SPARK_HOME/sbin/stop-all.sh ==

* 在处于standBy Master主节点执行
  * == $SPARK_HOME/sbin/stop-master.sh ==

##### 4.4.3.3 访问spark的webUI界面

- 访问master主节点web管理界面    http://node01:8080/

- 访问historyserver历史任务访问界面    http://node01:4000/

##### 4.4.3.4 其他安装

~~~
(1) 如何恢复到上一次活着master挂掉之前的状态?
	在高可用模式下，整个spark集群就有很多个master，其中只有一个master被zk选举成活着的master，其他的多个master都处于standby，同时把整个spark集群的元数据信息通过zk中节点进行保存。

	后期如果活着的master挂掉。首先zk会感知到活着的master挂掉，下面开始在多个处于standby中的master进行选举，再次产生一个活着的master，这个活着的master会读取保存在zk节点中的spark集群元数据信息，恢复到上一次master的状态。整个过程在恢复的时候经历过了很多个不同的阶段，每个阶段都需要一定时间，最终恢复到上个活着的master的转态，整个恢复过程一般需要1-2分钟。

(2) 在master的恢复阶段对任务的影响?

   a）对已经运行的任务是没有任何影响
   	  由于该任务正在运行，说明它已经拿到了计算资源，这个时候就不需要master。
   	  
   b) 对即将要提交的任务是有影响
   	  由于该任务需要有计算资源，这个时候会找活着的master去申请计算资源，由于没有一个活着的master,该任务是获取不到计算资源，也就是任务无法运行。
~~~

#### 4.4.4 spark集群的web管理界面(待修改)

* 当启动好spark集群之后，可以访问这样一个地址
  * http://master主机名:8080
  * 可以通过这个web界面观察到很多信息
    * 整个spark集群的详细信息
    * 整个spark集群总的资源信息
    * 整个spark集群已经使用的资源信息
    * 整个spark集群还剩的资源信息
    * 整个spark集群正在运行的任务信息
    * 整个spark集群已经完成的任务信息

![image-20200912172703247](spark.assets/image-20200912172703247.png)

#### 4.4.5 初识spark(Spark-Submit)

##### 4.4.5.1 Spark-Submit语法

```ruby
spark-submit \
--class <main-calss> \
--master <master-url> \
--deploy-mode <deploy-mode> \
--conf <key>=<value> \
...  #其他 options
<application-jar> \
[application-arguments]
```

| 参数名称               | 参数含义                                         | 参数默认值                                                   | 适用模式                      |
| ---------------------- | ------------------------------------------------ | ------------------------------------------------------------ | ----------------------------- |
| --master               | 指定主节访问路径                                 | spark://host:port, mesos://host:port, yarn,k8s://https://host:port, or local   默认local[*] | 不同模式使用不同值            |
| --deploy-mode          | 部署模式                                         | client ,cluster                                              | standAlone,on Yarn            |
| --class                | 指定main方法所在的class类                        | 无默认值                                                     | standAlone , on yarn          |
| --name                 | application的名称                                | 任意给定的名称                                               | standAlone,on  yarn           |
| --jars                 | 使用逗号隔开的列表，用以指定多个依赖的外部jar包  | 无默认值                                                     | standAlone,on yarn            |
| --packages             | 通过maven坐标来搜索jar包                         | 无默认值，一般不用这个参数                                   | standAlone , on yarn          |
| --exclude-packages     | 排除掉某些jar包                                  | 无默认值                                                     | standAlone , on yarn          |
| --repositories         | 与--packages参数搭配使用，用于搜索指定远程仓库   | 无默认值，一般不用这个参数                                   | standAlone , on yarn          |
| --files                | 逗号隔开的文件列表，用于传递文件到executor里面去 | 无默认值，可以用作广播变量                                   | standAlone , on yarn          |
| --conf                 | 指定spark的配置项                                | 无默认值                                                     | standAlone , on yarn          |
| --properties-file      | 指定spark的配置文件                              | 无默认值                                                     | standAlone , on yarn          |
| --driver-memory        | driver端内存大小                                 | 默认1024M，可以指定2G                                        | standAlone , on yarn          |
| --driver-java-options  | 额外的jar的配置选项传递到driver端                | 无默认值                                                     | standAlone , on yarn          |
| --driver-library-path  | driver端额外指定的jar包路径                      | 无默认值                                                     | standAlone , on yarn          |
| --driver-class-path    | 为driver端指定额外的class文件                    | 无默认值                                                     | standAlone , on yarn          |
| --executor-memory      | 每个executor的内存大小                           | 默认  1G                                                     | standAlone , on yarn          |
| --proxy-user           | 指定代理提交用户                                 | 无默认值                                                     | standAlone , on yarn          |
| --driver-cores         | driver的CPU核数                                  | 默认值  1                                                    | cluster模式使用               |
| --supervise            | 如果指定这个参数，任务失败就重启driver端         | 无默认值                                                     | spark standAlone  cluster模式 |
| --kill                 | 杀死指定的driver                                 | 无默认值                                                     | spark standAlone  cluster模式 |
| --status               | 查看driver的状态                                 | 无默认值                                                     | spark standAlone  cluster模式 |
| --total-executor-cores | 总共分配多少核CPU给所有的executor                | 无默认值                                                     | Spark standalone              |
| --executor-cores       | 每个executor分配的CPU核数                        |                                                              | standAlone  on  yarn          |
| --queue                | 提交到哪个yarn队列                               |                                                              | yarn                          |
| --num-executors        | 启动的executor的个数                             |                                                              | yarn                          |
| --archives             | 逗号隔开的列表                                   |                                                              | yarn                          |

##### 4.4.5.2 模式介绍

###### 4.4.5.2.1 Local模式

> Local模式(单机)就是在一台计算机上运行Spark，通常用于开发中。

- Submit提交方式

```ruby
bin/spark-submit \
--class org.apache.spark.examples.SparkPi \
--master local \
--executor-memory 2G \
--total-executor-cores 4 \
examples/jars/spark-examples_2.11-2.3.3.jar \
10
```

![image-20200912171454685](spark.assets/image-20200912171454685.png)

![image-20200912173505373](spark.assets/image-20200912173505373.png)

![image-20200912173137120](spark.assets/image-20200912173137120.png)

![image-20200912173200958](spark.assets/image-20200912173200958.png)

![image-20200912173231957](spark.assets/image-20200912173231957.png)

###### 4.4.5.2.2 Standalone模式

> 构建一个由 Master + Slave 构成的Spark集群，Spark运行在集群中，只依赖Spark，不依赖别的组件（如：Yarn）。(独立的Spark集群)

- Standalone-Client

```ruby
bin/spark-submit \
--class org.apache.spark.examples.SparkPi \
--master spark://node01:7077 \
--deploy-mode client \
--executor-memory 2G \
--total-executor-cores 4 \
examples/jars/spark-examples_2.11-2.3.3.jar \
10 

#多master提交
bin/spark-submit \
--class org.apache.spark.examples.SparkPi \
--master spark://node01:7077,node02:7077  \
--deploy-mode client \
--executor-memory 1G \
--total-executor-cores 2 \
examples/jars/spark-examples_2.11-2.3.3.jar \
10
```

![image-20200912173000463](spark.assets/image-20200912173000463.png)

![image-20200912173354702](spark.assets/image-20200912173354702.png)

- Standalone-Cluster

```ruby
bin/spark-submit \
--class org.apache.spark.examples.SparkPi \
--master spark://node01:7077  \
--deploy-mode cluster \
--executor-memory 1G \
--total-executor-cores 2 \
examples/jars/spark-examples_2.11-2.3.3.jar \
10
```

>spark集群中有很多个master，并不知道哪一个master是活着的master，即使你知道哪一个master是活着的master，它也有可能下一秒就挂掉，这里就可以把所有master都罗列出来
>--master spark://node01:7077,node02:7077,node03:7077
>
>后期程序会轮训整个master列表，最终找到活着的master，然后向它申请计算资源，最后运行程序。

![image-20200912173954127](spark.assets/image-20200912173954127.png)

![image-20200912174200884](spark.assets/image-20200912174200884.png)

- Standalone-Client流程图

![image-20200913132004359](spark.assets/image-20200913132004359.png)

1. 提交Spark-submit任务，启动应用程序
2. Driver初始化，在SparkContext启动过程中，先初始化DAGScheduler 和 TaskSchedulerImpl两个调度器， 
3. Driver向Master注册应用程序。Master收到注册消息后把应用放到待运行应用列表。
4. Master使用自己的`资源调度算法`分配Worker资源给应用程序。
5. 应用程序获得Worker时，Master会通知Worker中的WorkerEndpoint创建CoarseGrainedExecutorBackend进程，在该进程中创建执行容器Executor。
6. Executor创建完毕后发送消息到Master 和 DriverEndpoint。在SparkContext创建成功后， 等待Driver端发过来的任务。
7. SparkContext分配任务给CoarseGrainedExecutorBackend执行，在Executor上按照一定调度执行任务(这些任务就是代码)
8. CoarseGrainedExecutorBackend在处理任务的过程中把任务状态发送给SparkContext，SparkContext根据任务不同的结果进行处理。如果任务集处理完毕后，则继续发送其他任务集。
9. 应用程序运行完成后，SparkContext会进行资源回收。

- Standalone-Cluster流程图

![image-20200913132815118](spark.assets/image-20200913132815118.png)

###### 4.4.5.2.3 Yarn模式

> Spark客户端可以直接连接Yarn，不需要构建Spark集群。
>
> 有yarn-client和yarn-cluster两种模式，主要区别在：Driver程序的运行节点不同。
>
> yarn-client：Driver程序运行在客户端，适用于交互、调试，希望立即看见APP输出
>
> yarn-cluster：Driver程序运行在由ResourceManager启动的ApplicationMaster上，适用于生产环境
>
> http://spark.apache.org/docs/2.3.3/running-on-yarn.html

- Yarn-Client流程图

![image-20200913142926051](spark.assets/image-20200913142926051.png)

- Yarn-Cluster流程图

![image-20200913145056806](spark.assets/image-20200913145056806.png)

> <font color=red>需要在spark-env.sh文件中添加   HADOOP_CONF_DIR=/kkb/install/hadoop-2.6.0-cdh5.14.2/etc/hadoop</font>

- 客户端模式：Driver是在Client端，日志结果可以直接在后台看见

```ruby
bin/spark-submit \
--class org.apache.spark.examples.SparkPi \
--master yarn \
--deploy-mode client \
examples/jars/spark-examples_2.11-2.3.3.jar \
10
```

![image-20200912181554805](spark.assets/image-20200912181554805.png)

![image-20200912181713491](spark.assets/image-20200912181713491.png)

![image-20200912181653122](spark.assets/image-20200912181653122.png)

- 集群模式：Driver是在NodeManager端，日志结果需要通过监控日志查看

```ruby
bin/spark-submit \
--class org.apache.spark.examples.SparkPi \
--master yarn \
--deploy-mode cluster \
examples/jars/spark-examples_2.11-2.3.3.jar \
10
```

![image-20200912181913029](spark.assets/image-20200912181913029.png)

![image-20200912182000277](spark.assets/image-20200912182000277.png)

![image-20200912181855316](spark.assets/image-20200912181855316.png)

> 总结：
>
> client模式主要用来提交代码，查看结果，公司中常使用一台单独的提交机器来进行任务提交。
>
> cluster模式主要用在一些数据持久化，不需要查看结果的情况。

#### 4.4.6 初识spark(Spark-Shell)

##### 4.4.6.1 WordCount案例

- 节点数据准备

```java
#创建word.txt文件
vim word.txt
#--->
hadoop hello spark
spark word
hello hadoop spark
#---<
#上传HDFS集群
hadoop dfs -put word.txt /
```

```java
#链接客户端
bin/spark-shell
```

![image-20200912182704054](spark.assets/image-20200912182704054.png)

```ruby
sc.textFile("/word.txt").flatMap(line => line.split(' ')).map((_,1)).reduceByKey(_ + _).collect
```

![image-20200912182725556](spark.assets/image-20200912182725556.png)

> 每个Spark应用程序都包含一个驱动程序，驱动程序负责把并行操作发布到集群上，驱动程序包含Spark应用中的主函数，定义了分布式数据集以应用在集群中，在wordcount案例中，spark-shell就是我们的驱动程序，所以我们键入我们任何想要的操作，然后由它负责发布，驱动程序通过SparkContext对象来访问Spark，SparkContext对象相当于一个到Spark集群的链接

##### 4.4.6.2  WordCount案例分析

> 根据spark-shell提供的UI进入查看应用，或者应用执行完后，停止spark-shell进入历史日志UI查询。

1. 一个行动算子collect（），一个job，如下源码中可以看出runJob

![](https://img2020.cnblogs.com/blog/1235870/202009/1235870-20200911150130329-907696634.png)

- 通过提示的4040端口访问，或者停止shell后在历史UI查看

![image-20200912182900292](spark.assets/image-20200912182900292.png)

2. 一次宽依赖shuffle算子reduceByKey（），切分成2个Stage阶段，如下源码中很明显可以看出有Shuffle参与

![](https://img2020.cnblogs.com/blog/1235870/202009/1235870-20200911151000667-1185693476.png)

![](https://img2020.cnblogs.com/blog/1235870/202009/1235870-20200911150737438-1891291519.png)

![image-20200912182923577](spark.assets/image-20200912182923577.png)

> Stage阶段，默认文件被切分成2份，所以有2个task
>
> Stage阶段0

![image-20200912183020534](spark.assets/image-20200912183020534.png)

> Stage阶段1

![image-20200912183037309](spark.assets/image-20200912183037309.png)

##### 4.4.6.3 Shuffle洗牌

![](https://img2020.cnblogs.com/blog/1235870/202009/1235870-20200911153423040-457705263.png)

- 在划分stage时，最后一个stage称为FinalStage，本质上是一个ResultStage对象，前面所有的stage被称为ShuffleMapStage 

- <font color=red>ShuffleMapStage 的结束伴随着shuffle文件写磁盘</font>
- <font color=red>ResultStage对应代码中的action算子，即将一个函数应用在RDD的各个Partition（分区）的数据集上，意味着一个Job运行结束</font>

### 4.5  使用IDEA开发Spark

- 构建maven工程，添加pom依赖

```xml
    <dependencies>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-core_2.11</artifactId>
            <version>2.3.3</version>
        </dependency>
        <!--如果插件下不下来则将这个打开-->
        <!--        <dependency>-->
        <!--            <groupId>net.alchim31.maven</groupId>-->
        <!--            <artifactId>scala-maven-plugin</artifactId>-->
        <!--            <version>3.2.2</version>-->
        <!--        </dependency>-->
        <!--如果插件下不下来则将这个打开-->
        <!--        <dependency>-->
        <!--            <groupId>org.apache.maven.plugins</groupId>-->
        <!--            <artifactId>maven-shade-plugin</artifactId>-->
        <!--            <version>2.4.3</version>-->
        <!--        </dependency>-->
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>net.alchim31.maven</groupId>
                <artifactId>scala-maven-plugin</artifactId>
                <version>3.2.2</version>
                <executions>
                    <execution>
                        <goals>
                            <goal>compile</goal>
                            <goal>testCompile</goal>
                        </goals>
                        <configuration>
                            <args>
                                <arg>-dependencyfile</arg>
                                <arg>${project.build.directory}/.scala_dependencies</arg>
                            </args>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-shade-plugin</artifactId>
                <version>2.4.3</version>
                <executions>
                    <execution>
                        <phase>package</phase>
                        <goals>
                            <goal>shade</goal>
                        </goals>
                        <configuration>
                            <filters>
                                <filter>
                                    <artifact>*:*</artifact>
                                    <excludes>
                                        <exclude>META-INF/*.SF</exclude>
                                        <exclude>META-INF/*.DSA</exclude>
                                        <exclude>META-INF/*.RSA</exclude>
                                    </excludes>
                                </filter>
                            </filters>
                            <transformers>
                                <transformer
                                        implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                                    <mainClass></mainClass>
                                </transformer>
                            </transformers>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
```



#### 4.5.1 使用Java语言开发

- 在resources文件夹下，新建word.csv文件

```csv
hello,spark
hello,scala,hadoop
hello,hdfs
hello,spark,hadoop
hello
```

```java
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

//todo: 利用java语言开发spark的单词统计程序
public class JavaWordCount {
    public static void main(String[] args) {
        //1、创建SparkConf对象
        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount").setMaster("local[2]");

        //2、构建JavaSparkContext对象
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        //3、读取数据文件
        JavaRDD<String> data = jsc.textFile(JavaWordCount.class.getClassLoader().getSystemResource("word.csv").getPath());

        //4、切分每一行获取所有的单词   scala:  data.flatMap(x=>x.split(" "))
        JavaRDD<String> wordsJavaRDD = data.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String line) throws Exception {
                String[] words = line.split(",");
                return Arrays.asList(words).iterator();
            }
        });

        //5、每个单词计为1    scala:  wordsJavaRDD.map(x=>(x,1))
        JavaPairRDD<String, Integer> wordAndOne = wordsJavaRDD.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });

        //6、相同单词出现的1累加    scala:  wordAndOne.reduceByKey((x,y)=>x+y)
        JavaPairRDD<String, Integer> result = wordAndOne.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        //按照单词出现的次数降序 (单词，次数)  -->(次数,单词).sortByKey----> (单词，次数)
        JavaPairRDD<Integer, String> reverseJavaRDD = result.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
                return new Tuple2<Integer, String>(t._2, t._1);
            }
        });

        JavaPairRDD<String, Integer> sortedRDD = reverseJavaRDD.sortByKey(false).mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            public Tuple2<String, Integer> call(Tuple2<Integer, String> t) throws Exception {
                return new Tuple2<String, Integer>(t._2, t._1);
            }
        });

        //7、收集打印
        List<Tuple2<String, Integer>> finalResult = sortedRDD.collect();

        for (Tuple2<String, Integer> t : finalResult) {
            System.out.println("单词："+t._1 +"\t次数："+t._2);
        }

        jsc.stop();

    }
}
```

![image-20200912221823119](spark.assets/image-20200912221823119.png)

#### 4.5.2 使用Scala语言开发

```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WorkCount").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val tuples: Array[(String, Int)] = sc.textFile(ClassLoader.getSystemResource("word.csv").getPath)
      .flatMap(_.split(","))
      .map((_, 1))
      .reduceByKey(_ + _)
      .collect()
    tuples.foreach(println)
    sc.stop()
  }
}

#将地址换成hdfs://node01:8020/word.txt，切分改成空格
#输出结果：
>>(word,1)
>>(hello,2)
>>>>(spark,3)
>>(hadoop,2)
```

![image-20200912220658568](spark.assets/image-20200912220658568.png)

#### 4.5.3 上传jar包提交Spark任务

- 修改单词统计程序，并打成jar包上传到node01服务器的/kkb/install路径下

```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WorkCount") //.setMaster("local[*]")
    val sc = new SparkContext(conf)
    //    val tuples: Array[(String, Int)] = sc.textFile("hdfs://cdh01.cm:8020/word.txt")
    val resultRDD: RDD[(String, Int)] = sc.textFile(args(0))
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
    //    .collect()
    //    tuples.foreach(println)
    resultRDD.saveAsTextFile(args(1))
    sc.stop()
  }
}
```

- 将我们修改之后的程序打包，打包之后上传到node01服务器的/kkb/install路径下

![image-20200913121022544](spark.assets/image-20200913121022544.png)

- 提交spark任务

> 注意如果是cluster模式，需要将jar包上传到hdfs上，路径为->hdfs://node01:8020/xxx.jar
>
> 前面的圆周率任务可以执行的原因是jar包每台机器都有，所以使用集群模式提交任务，要不使用hdfs集群上的jar，要不就需要将jar包放在每台机器上。

```shell
bin/spark-submit --master  spark://node01:7077 \
--deploy-mode client \
--class com.kkb.spark.count.SparkCount  \
--executor-memory 1g \
--total-executor-cores 2 \
/kkb/install/original-sparkdemo-1.0-SNAPSHOT.jar \
hdfs://node01:8020/word.txt hdfs://node01:8020/output
```

![image-20200913122341899](spark.assets/image-20200913122341899.png)

### 4.6 RDD基本介绍

#### 4.6.1 什么是RDD

> Resilient Distributed DataSet：弹性分布式数据集，是Spark中最基本数据抽象，可以理解为数据集合。
>
> - **Dataset**:         数据集合，存储很多数据.
> - **Distributed**：RDD内部的元素进行了分布式存储，方便于后期进行分布式计算.
> - **Resilient**：     表示弹性，RDD的数据是可以保存在内存或者是磁盘中.
>
> 在代码中是一个抽象类，它代表一个弹性的、不可变的、可分区，里面的元素可并行计算的集合。

#### 4.6.2 RDD的五个主要特性

![image-20200912195625511](spark.assets/image-20200912195625511.png)

1. A list of partitions(分区性)

   - RDD有很多分区，每一个分区内部是包含了该RDD的部分数据
   - 因为有多个分区，那么一个分区（Partition）列表，就可以看作是数据集的基本组成单位

   - spark中任务是以task线程的方式运行，对于RDD来说，每个分区都会被一个计算任务处理， 一个分区就对应一个task线程，故分区就决定了并行计算的粒度。
   - 用户可以在创建RDD时，指定RDD的分区数，如果没有指定，那么采用默认值（程序所分配到的CPU Coure的数目）
   - 每个分配的储存是由BlockManager实现的，每个分区都会被逻辑映射成BlockManager的一个Block，而这个Block会被一个Task负责计算。

2. A function for computing each split(计算每个分区的函数)

   - Spark中RDD的计算是以分区为单位的，每个RDD都会实现compute函数以达到这个目的

3. A list of dependencies on other RDDs(依赖性--一个rdd会依赖于其他多个rdd)

   - spark任务的容错机制就是根据这个特性（血统）而来。
   - RDD的每次转换都会生成一个新的RDD，所以RDD之间会形成类似于流水线一样的前后依赖关系，在部分分区数据丢失时，Spark可以通过这个依赖关系重新计算丢失的分区数据，而不是对RDD的所有分区进行重新计算。

4. Optionally, a Partitioner for key-value RDDs (e.g. to say that the RDD is hash-partitioned)--(对储存键值对的RDD，还有一个可选的分区器)
   - 只有对于key-value的RDD(RDD[(String, Int)]),并且产生shuffle，才会有Partitioner，非key-value的RDD(RDD[String])的Parititioner的值是None。
   - Partitioner不但决定了RDD的分区数量，也决定了parent RDD Shuffle输出时的分区数量
   - 当前Spark中实现了两种类型的分区函数，一个是基于哈希的HashPartitioner，(key.hashcode % 分区数= 分区号)。它是默认值，另外一个是基于范围的RangePartitioner。

5. Optionally, a list of preferred locations to compute each split on (e.g. block locations for an HDFS file)--(储存每个分区优先位置的列表(本地计算性))

   - 比如对于一个HDFS文件来说，这个列表保存的就是每个Partition所在文件快的位置，按照“移动数据不如移动计算”的理念，Spark在进行任务调度的时候，会尽可能地将计算任务分配到其所要处理数据块的储存位置，减少数据的网络传输，提升计算效率。

#### 4.6.3 基于spark的单词统计程序剖析rdd的五大属性

- 需求

  ```
  HDFS上有一个大小为300M的文件，通过spark实现文件单词统计，最后把结果数据保存到HDFS上
  ```

- 代码

  ```scala
  sc.textFile("/words.txt").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).saveAsTextFile("/out")
  ```

- 流程分析

![image-20200913151831757](spark.assets/image-20200913151831757.png)

#### 4.6.4 Transformation和Action算子

> 在Spark中，Transformation算子（也称转换算子)，在没有Action算子（也称行动算子)去触发的时候，是不会执行的，可以理解为懒算子，而Action算子可以理解为触发算子。
>
> 还有一种Shuffle类算子，就是上面说到的洗牌算子。

##### 4.6.4.1 Action算子

| **动作**                       | **含义**                                                     |
| ------------------------------ | ------------------------------------------------------------ |
| **reduce(func)**               | reduce将RDD中元素前两个传给输入函数，产生一个新的return值，新产生的return值与RDD中下一个元素（第三个元素）组成两个元素，再被传给输入函数，直到最后只有一个值为止。 |
| **collect()**                  | 在驱动程序中，以数组的形式返回数据集的所有元素               |
| **count()**                    | 返回RDD的元素个数                                            |
| **first()**                    | 返回RDD的第一个元素（类似于take(1)）                         |
| **take(n)**                    | 返回一个由数据集的前n个元素组成的数组                        |
| **takeOrdered(n, [ordering])** | 返回自然顺序或者自定义顺序的前 n 个元素                      |
| **saveAsTextFile(path)**       | 将数据集的元素以textfile的形式保存到HDFS文件系统或者其他支持的文件系统，对于每个元素，Spark将会调用toString方法，将它装换为文件中的文本 |
| **saveAsSequenceFile(path)**   | 将数据集中的元素以Hadoop sequencefile的格式保存到指定的目录下，可以使HDFS或者其他Hadoop支持的文件系统。 |
| **saveAsObjectFile(path)**     | 将数据集的元素，以 Java 序列化的方式保存到指定的目录下       |
| **countByKey()**               | 针对(K,V)类型的RDD，返回一个(K,Int)的map，表示每一个key对应的元素个数。 |
| **foreach(func)**              | 在数据集的每一个元素上，运行函数func                         |
| **foreachPartition(func)**     | 在数据集的每一个分区上，运行函数func                         |

##### 4.6.4.2 Transformation算子

| **转换**                                            | **含义**                                                     |
| --------------------------------------------------- | ------------------------------------------------------------ |
| **map(func)**                                       | 返回一个新的RDD，该RDD由每一个输入元素经过func函数转换后组成 |
| **filter(func)**                                    | 返回一个新的RDD，该RDD由经过func函数计算后返回值为true的输入元素组成 |
| **flatMap(func)**                                   | 类似于map，但是每一个输入元素可以被映射为0或多个输出元素（所以func应该返回一个序列，而不是单一元素） |
| **mapPartitions(func)**                             | 类似于map，但独立地在RDD的每一个分片上运行，因此在类型为T的RDD上运行时，func的函数类型必须是Iterator[T] => Iterator[U] |
| **mapPartitionsWithIndex(func)**                    | 类似于mapPartitions，但func带有一个整数参数表示分片的索引值，因此在类型为T的RDD上运行时，func的函数类型必须是(Int, Interator[T]) => Iterator[U] |
| **union(otherDataset)**                             | 对源RDD和参数RDD求并集后返回一个新的RDD                      |
| **intersection(otherDataset)**                      | 对源RDD和参数RDD求交集后返回一个新的RDD                      |
| **distinct([numTasks]))**                           | 对源RDD进行去重后返回一个新的RDD                             |
| **groupByKey([numTasks])**                          | 在一个(K,V)的RDD上调用，返回一个(K, Iterator[V])的RDD        |
| **reduceByKey(func, [numTasks])**                   | 在一个(K,V)的RDD上调用，返回一个(K,V)的RDD，使用指定的reduce函数，将相同key的值聚合到一起，与groupByKey类似，reduce任务的个数可以通过第二个可选的参数来设置 |
| **sortByKey([ascending], [numTasks])**              | 在一个(K,V)的RDD上调用，K必须实现Ordered接口，返回一个按照key进行排序的(K,V)的RDD |
| **sortBy(func,[ascending], [numTasks])**            | 与sortByKey类似，但是更灵活                                  |
| **join(otherDataset, [numTasks])**                  | 在类型为(K,V)和(K,W)的RDD上调用，返回一个相同key对应的所有元素对在一起的(K,(V,W))的RDD |
| **cogroup(otherDataset, [numTasks])**               | 在类型为(K,V)和(K,W)的RDD上调用，返回一个(K,(Iterable<V>,Iterable<W>))类型的RDD |
| **coalesce(numPartitions)**                         | 减少 RDD 的分区数到指定值。                                  |
| **repartition(numPartitions)**                      | 重新给 RDD 分区                                              |
| **repartitionAndSortWithinPartitions(partitioner)** | 重新给 RDD 分区，并且每个分区内以记录的 key 排序             |

##### 4.6.4.3 Shuffle算子

去重

```scala
def distinct()
def distinct(numPartitions: Int)
```

聚合

```scala
def reduceByKey(func: (V, V) => V, numPartitions: Int): RDD[(K, V)]
def reduceByKey(partitioner: Partitioner, func: (V, V) => V): RDD[(K, V)]
def groupBy[K](f: T => K, p: Partitioner):RDD[(K, Iterable[V])]
def groupByKey(partitioner: Partitioner):RDD[(K, Iterable[V])]
def aggregateByKey[U: ClassTag](zeroValue: U, partitioner: Partitioner): RDD[(K, U)]
def aggregateByKey[U: ClassTag](zeroValue: U, numPartitions: Int): RDD[(K, U)]
def combineByKey[C](createCombiner: V => C, mergeValue: (C, V) => C, mergeCombiners: (C, C) => C): RDD[(K, C)]
def combineByKey[C](createCombiner: V => C, mergeValue: (C, V) => C, mergeCombiners: (C, C) => C, numPartitions: Int): RDD[(K, C)]
def combineByKey[C](createCombiner: V => C, mergeValue: (C, V) => C, mergeCombiners: (C, C) => C, partitioner: Partitioner, mapSideCombine: Boolean = true, serializer: Serializer = null): RDD[(K, C)]
```

排序

```scala
def sortByKey(ascending: Boolean = true, numPartitions: Int = self.partitions.length): RDD[(K, V)]
def sortBy[K](f: (T) => K, ascending: Boolean = true, numPartitions: Int = this.partitions.length)(implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T]
```

重分区

```scala
def coalesce(numPartitions: Int, shuffle: Boolean = false, partitionCoalescer: Option[PartitionCoalescer] = Option.empty)
def repartition(numPartitions: Int)(implicit ord: Ordering[T] = null)
```

集合或者表操作

```scala
def intersection(other: RDD[T]): RDD[T]
def intersection(other: RDD[T], partitioner: Partitioner)(implicit ord: Ordering[T] = null): RDD[T]
def intersection(other: RDD[T], numPartitions: Int): RDD[T]
def subtract(other: RDD[T], numPartitions: Int): RDD[T]
def subtract(other: RDD[T], p: Partitioner)(implicit ord: Ordering[T] = null): RDD[T]
def subtractByKey[W: ClassTag](other: RDD[(K, W)]): RDD[(K, V)]
def subtractByKey[W: ClassTag](other: RDD[(K, W)], numPartitions: Int): RDD[(K, V)]
def subtractByKey[W: ClassTag](other: RDD[(K, W)], p: Partitioner): RDD[(K, V)]
def join[W](other: RDD[(K, W)], partitioner: Partitioner): RDD[(K, (V, W))]
def join[W](other: RDD[(K, W)]): RDD[(K, (V, W))]
def join[W](other: RDD[(K, W)], numPartitions: Int): RDD[(K, (V, W))]
def leftOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (V, Option[W]))]
```

#### 4.6.5 RDD的创建方式

##### 4.6.5.1 通过已经存在的scala集合去构建

- 前期做一些测试用到的比较多

```scala
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Demo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WorkCount").setMaster("local[*]")
    val sc = new SparkContext(conf)

    /**
     * 传入分区数为1，结果数序打印
     * 传入分区数大于1，结果顺序不定，因为数据被打散在N个分区里
     * */
    val rdd1: RDD[Int] = sc.parallelize(1.to(10), 1)
    print("rdd1->:")
    rdd1.foreach(x => print(x + "\t"))
    println("")

    val rdd2=sc.parallelize(List(1,2,3,4,5),2)
    print("rdd2->:")
    rdd2.foreach(x => print(x + "\t"))
    println("")

    val rdd3=sc.parallelize(Array("hadoop","hive","spark"),1)
    print("rdd3->:")
    rdd3.foreach(x => print(x + "\t"))
    println("")

    val rdd4=sc.makeRDD(List(1,2,3,4),2)
    print("rdd4->:")
    rdd4.foreach(x => print(x + "\t"))
    println("")

    sc.stop()
  }
}
```



##### 4.6.5.2 加载外部的数据源去构建

- 读取textFile

> WordCount案例介绍了此种用法

-  读取Json文件

> 在idea中，resources目录下创建word.json文件

```json
{"name": "zhangsa"}
{"name": "lisi", "age": 30}
{"name": "wangwu"}
["aa","bb"]
```

```ruby
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

object Demo1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("json").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd1: RDD[String] = sc.textFile(this.getClass().getClassLoader.getResource("word.json").getPath)
    val rdd2: RDD[Option[Any]] = rdd1.map(JSON.parseFull(_))
    rdd2.foreach(println)
  }
}
```

- 读取Object对象文件

```ruby
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("object").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd1: RDD[Int] = sc.parallelize(Array(1,2,3,4,5))
    rdd1.saveAsObjectFile("hdfs://cdh01.cm/test")

    val rdd2: RDD[Nothing] = sc.objectFile("hdfs://cdh01.cm/test")
    rdd2.foreach(println)
  }
}
```

##### 4.6.5.3 从其他RDD转换得到新的RDD

- 案例一：

```ruby
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("map").setMaster("local[*]")
    val sc = new SparkContext(conf)

    /**
     * map算子，一共有多少元素就会执行多少次，和分区数无关，修改分区数进行测试
     **/
    val rdd: RDD[Int] = sc.parallelize(1.to(5), 1)
    val mapRdd: RDD[Int] = rdd.map(x => {
      println("执行") //一共被执行5次
      x * 2
    })
    val result: Array[Int] = mapRdd.collect()
    result.foreach(x => print(x + "\t"))
  }
}
```

- 案例二：

```ruby
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("mapPartitions").setMaster("local[*]")
    val sc = new SparkContext(conf)

    /**
      * mapPartitions算子，一个分区内处理，几个分区就执行几次，优于map函数，常用于时间转换，数据库连接
      **/
    val rdd: RDD[Int] = sc.parallelize(1.to(10), 2)
    val mapRdd: RDD[Int] = rdd.mapPartitions(it => {
      println("执行") //分区2次，共打印2次
      it.map(x => x * 2)
    })
    val result: Array[Int] = mapRdd.collect()
    result.foreach(x => print(x + "\t")) 
  }
}
```

- 案例三：

```ruby
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo5 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("mapPartitionsWithIndex").setMaster("local[*]")
    val sc = new SparkContext(conf)

    /**
      * mapPartitionsWithIndex算子，一个分区内处理，几个分区就执行几次，返回带有分区号的结果集
      **/
    val rdd: RDD[Int] = sc.parallelize(1.to(10), 2)
    val value: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex((index, it) => {
      println("执行") //执行两次
      it.map((index, _))
    })
    val result: Array[(Int, Int)] = value.collect()
    result.foreach(x => print(x + "\t"))
  }
}
```









