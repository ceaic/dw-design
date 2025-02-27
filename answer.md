### 1.实时数据处理与分析

#### a

游戏服务器 -> Filebeat -> Kafka -> Spark Streaming -> 外部存储 -> Tableau

说明：

1.游戏服务器将登录和登出日志和付费日志打到本地日志文件中；

2.配置 Filebeat 监听日志文件，将实时产生的日志发送到 Kafka 的 Topic 中；

3.使用 Spark Streaming 消费 Kafka 数据，将用户的登录登出状态和付费保存到 Redis（状态维护）和外部存储（查询）中，下一批次数据与 Redis 中保存的用户登录数据和付费数据进行计算，得到用户在线时长、付费金额后，再次写入 Redis 中进行状态维护，以及写入外部存储中供查询分析。

4.tableau 连接外部存储，展示指标数据。

#### b

```scala
        val spark = SparkSession.builder()
            .appName("app-name")
            .getOrCreate()
        val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
        // Kafka配置
        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> "kafkahost:9092",
            ...
          )
				// 创建输入流
        val stream = KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](
                Array("topic-name"), 
                kafkaParams
            )
        )

        // 处理每个批次的数据
        stream.foreachPartition { par =>
            if (par.nonEmpty) {   
                // 创建连接
                val jedis: Jedis = getJedis
              	val jedis: Jedis = getJedis

                par.foreach { data =>
                  events.foreach{ event =>
                  	// 解析事件数据
                    eventType, userId, eventTime, amount
                    
                    // 计算新状态
                    eventType match {
                        case "LOGIN" => {
                          //更新 Redis 的状态
                          lastSession = 0
                          if jedis.exists(login_${userId}) {
                            lastSession = jedis.get('login_${userId}').spilt(':')[0]
                          } else
                          	jedis.set('login_${userId}', lastSession + ':' + eventTime)
                          //更新 MySQL 的状态
                          //
                        }
                        case "LOGOUT" => {
                          //获取原有登录数据，和原有登录数据作差，求在线时长
                          (lastSession, loginTime) = jedis.get('login_${userId}').spilt(':')[0], [1]
                          session = lastSession + (eventTime - loginTime)
                          //更新 Redis 的状态
                          jedis.set('login_${userId}', session + ':' + eventTime)
                          //更新 MySQL 的状态
                          //
                        }
                        case "PAYMENT" => {
                          //获取原有付费数据，求和
                          //更新 Redis 的状态
                          //更新 MySQL 的状态
                        }
                      }
                   }
                }
              	jedis.close
            }
        }
        
        // 启动流处理
        ssc.start()
        ssc.awaitTermination()
    }
}

```



#### c

低延迟：缩小批处理间隔可以提高实时性，但会增加计算和写入频次和造成更多IO，需要从性能和延迟之间权衡。

可靠性：启用 checkpoint 、将 Kafka 偏移量持久化到外部存储可以保障故障恢复时从上次的断点继续消费数据；或通过幂等写入存储层保障 ExectlyOnce。

流量高峰应对：一方面 Kafka 可以实现削峰平谷的作用，确保在数据流量高峰时期不会对计算引擎和存储系统产生较大的资源压力；另一方面可以通过云计算或者 Kubernetes 的动态资源调整的功能来实现资源弹性配置，在高峰时期自动增加计算资源来应对高峰流量。



### 2.数据仓库与BI系统设计

#### a

1.梳理业务过程：确定业务目标，也就是分析师或者业务部门要什么？如用户活跃情况、新关卡的表现、限时活动的效果、道具消耗等。

2.确定粒度：数据的颗粒度/细化程度，比如某个玩家在某个时间点、某关卡消耗了1个游戏道具，就是一个原子事件，也是最细粒度的一条数据。

3.确定维度：选取业务所需的分析问题的角度，比如从国家角度进行分析，查看不同国家的玩家对某个活动的参与度，这里的国家就是一个维度。需要保证维度的一致性，即相同维度具有统一命名，减少理解和使用成本。

4.设计事实表：通过确定的粒度和维度，来描述某个业务事实的表现，如通关时间、消耗道具数量、游戏步数等。

可以通过在事实表中进行维度退化，来实现支持多维分析的需求。

#### b

1.tableau：需要付费，功能丰富，支持的数据源多

2.superset：开源免费，定制化强，需要开发和维护，有一定门槛

3.quicksight：与 AWS 集成比较好

推荐使用 tableau，入门简单，功能丰富可以支持各种复杂的数据分析场景。

#### c

1.在数据接入方面，建立数据改动知会机制，确保原始数据发生改变之后数据部门能够第一时间进行计算口径更新。

2.在数据仓库层面，确保数据的准确和完整，通过跨平台数据对比或者同比环比监测数据波动情况；建立全平台的一致性维度和指标，消除数据解读上的歧义；公共逻辑下沉，避免重复开发，产生数据孤岛和烟囱式开发问题，减少计算复杂度。

3.在指标开发和展示阶段，与业务部门共同搭建指标体系，梳理数据血缘，对指标数据统一管理，方便问题溯源，将时间花在解决问题而非查找问题上。

4.深入了解业务，深度体验游戏，减少需求沟通成本，提高需求开发效率。



### 3.数据分析与产品改进

#### a

通过查询关卡进入、关卡完成的数据，关卡中途退出的时机、关内道具资源消耗、关内连续点击之间时间间隔等数据，或者对玩家进行分群（设备、等级等）分析，来查找是否存在设计缺陷

#### b

1.降低关卡难度（增加行动步数/增加临时道具）

测试方案：a.原难度；b.增加玩家的行动步数

2.提高通关奖励，激励玩家完成关卡的动力

测试方案：a.原有通关奖励；b.增加奖励

3.更换关卡地图、进行UI调整增加提示

测试方案：a.原有地图/UI；b.更换同等难度的其他地图或UI

#### c

1.设置关卡评分系统，通过限时道具或体力促使玩家对关卡进行评分

2.拉取商店评分数据、客诉数据分析玩家遇到的问题和诉求

3.可以通过发放回归奖励等方式对流失的高价值玩家进行召回，提高用户留存