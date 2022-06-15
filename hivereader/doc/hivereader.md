# DataX HiveReader 插件文档

## 介绍

通过Shell执行自定义Hive查询SQL，写入临时表(ORCFILE)，再将临时表数据给到DataX，最后删除。

## 要求

创建必须的临时目录：

```shell
hdfs dfs -mkdir -p /tmp/datax-hivereader
```

## 配置

```json
{
    "job": {
        "setting": {
            "speed": {
                "channel": "3"
            },
            "errorLimit": {
                "record": "0",
                "percentage": "0.02"
            }
        },
        "content": [
            {
                "reader": {
                    "name": "hivereader",
                    "parameter": {
                        "defaultFS": "hdfs://xxx:port",
                        "zkquorum":"jdbc:hive2://xxx:2181,xxx:2181,xxx:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2",
                        "username":"xxxx",
                        "password":"xxxx",
                        "sql": "select * from user",
                        "hadoopConfig": {
                        },
                        "haveKerberos": false,
                        "kerberosKeytabFilePath": "",
                        "kerberosPrincipal": ""
                    }
                },
                "writer": {
                }
            }
        ]
    }
}
```

## 参数说明

* **defaultFS**

  * 描述：Hadoop hdfs文件系统namenode节点地址。
  * 此参数为执行SQL时，使用hive -e 模式执行，此参数与zkquorum选择填写一个即可，都填则优先读取。

    **目前HiveReader已经支持Kerberos认证，如果需要权限认证，则需要用户配置kerberos参数，见下面**

  * 必选：是  (与zkquorum选填一个即可）

  * 默认值：无

* **zkquorum**

  * 描述：Hive集群文件系统对应ZK集群。
  * 此参数为执行SQL时，使用beeline -e模式执行，当defaultFS不填写时，或者hive -e模式执行失败时生效。

  * 必选：是  (与defaultFS选填一个即可）

  * 默认值：无

* **username**

  * 描述：hive集群验证用户名
  * 此参数为执行SQL时，使用beeline -e模式执行时使用，当defaultFS不填写时，或者hive -e模式执行失败时生效。

  * 必选：否  (当zkquorum填写时，必填）

  * 默认值：无

* **password**

  * 描述：Hive集群验证密码。
  * 此参数为执行SQL时，使用beeline -e模式执行时使用，当defaultFS不填写时，或者hive -e模式执行失败时生效。

  * 必选：否  (当zkquorum填写时，必填）

  * 默认值：无

* **sql**

  * 描述：自定义sql查询语句，会将查询结果存到临时表，最后供下游系统读取，最后会删除临时表，例如：


    ```sql
    select id, username, age, sex from user
    ```

  * 必选：是

  * 默认值：无

* **hadoopConfig**

  * 描述：hadoopConfig里可以配置与Hadoop相关的一些高级参数，比如HA的配置。

    ```json
    "hadoopConfig": {
        "dfs.nameservices": "testDfs",
        "dfs.ha.namenodes.testDfs": "namenode1,namenode2",
        "dfs.namenode.rpc-address.aliDfs.namenode1": "",
        "dfs.namenode.rpc-address.aliDfs.namenode2": "",
        "dfs.client.failover.proxy.provider.testDfs": "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
    }
    ```

  * 必选：否

  * 默认值：无

* **haveKerberos**

  * 描述：是否有Kerberos认证，默认false

     例如如果用户配置true，则配置项`kerberosKeytabFilePath`，`kerberosPrincipal`为必填。

  * 必选：`haveKerberos `为true必选

  * 默认值：false

* **kerberosKeytabFilePath**

  * 描述：Kerberos认证 keytab文件路径，绝对路径

  * 必选：`haveKerberos`为true必选

  * 默认值：无

* **kerberosPrincipal**

  * 描述：Kerberos认证Principal名，如xxxx/hadoopclient@xxx.xxx

  * 必选：`haveKerberos`为true必选

  * 默认值：无
