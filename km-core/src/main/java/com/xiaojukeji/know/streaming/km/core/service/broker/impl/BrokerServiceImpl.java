package com.xiaojukeji.know.streaming.km.core.service.broker.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.didiglobal.logi.log.ILog;
import com.didiglobal.logi.log.LogFactory;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.xiaojukeji.know.streaming.km.common.bean.entity.broker.Broker;
import com.xiaojukeji.know.streaming.km.common.bean.entity.cluster.ClusterPhy;
import com.xiaojukeji.know.streaming.km.common.bean.entity.config.JmxConfig;
import com.xiaojukeji.know.streaming.km.common.bean.entity.param.VersionItemParam;
import com.xiaojukeji.know.streaming.km.common.bean.entity.param.broker.BrokerParam;
import com.xiaojukeji.know.streaming.km.common.bean.entity.result.Result;
import com.xiaojukeji.know.streaming.km.common.bean.entity.result.ResultStatus;
import com.xiaojukeji.know.streaming.km.common.bean.entity.topic.Topic;
import com.xiaojukeji.know.streaming.km.common.bean.po.broker.BrokerPO;
import com.xiaojukeji.know.streaming.km.common.constant.Constant;
import com.xiaojukeji.know.streaming.km.common.constant.KafkaConstant;
import com.xiaojukeji.know.streaming.km.common.constant.MsgConstant;
import com.xiaojukeji.know.streaming.km.common.enums.cluster.ClusterRunStateEnum;
import com.xiaojukeji.know.streaming.km.common.enums.version.VersionItemTypeEnum;
import com.xiaojukeji.know.streaming.km.common.exception.NotExistException;
import com.xiaojukeji.know.streaming.km.common.exception.VCHandlerNotExistException;
import com.xiaojukeji.know.streaming.km.common.jmx.JmxConnectorWrap;
import com.xiaojukeji.know.streaming.km.common.utils.ConvertUtil;
import com.xiaojukeji.know.streaming.km.common.utils.ValidateUtils;
import com.xiaojukeji.know.streaming.km.core.service.broker.BrokerService;
import com.xiaojukeji.know.streaming.km.core.service.topic.TopicService;
import com.xiaojukeji.know.streaming.km.core.service.version.BaseKafkaVersionControlService;
import com.xiaojukeji.know.streaming.km.persistence.jmx.JmxDAO;
import com.xiaojukeji.know.streaming.km.persistence.kafka.KafkaAdminClient;
import com.xiaojukeji.know.streaming.km.persistence.kafka.KafkaJMXClient;
import com.xiaojukeji.know.streaming.km.persistence.kafka.zookeeper.service.KafkaZKDAO;
import com.xiaojukeji.know.streaming.km.persistence.mysql.broker.BrokerDAO;
import kafka.zk.BrokerIdsZNode;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.Node;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.management.ObjectName;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.xiaojukeji.know.streaming.km.common.bean.entity.result.ResultStatus.VC_HANDLE_NOT_EXIST;
import static com.xiaojukeji.know.streaming.km.common.enums.version.VersionEnum.V_1_0_0;
import static com.xiaojukeji.know.streaming.km.common.enums.version.VersionEnum.V_MAX;
import static com.xiaojukeji.know.streaming.km.common.enums.version.VersionItemTypeEnum.SERVICE_SEARCH_BROKER;
import static com.xiaojukeji.know.streaming.km.common.jmx.JmxAttribute.VERSION;
import static com.xiaojukeji.know.streaming.km.common.jmx.JmxName.JMX_SERVER_APP_INFO;

@Service
public class BrokerServiceImpl extends BaseKafkaVersionControlService implements BrokerService {
    private static final ILog log = LogFactory.getLog(BrokerServiceImpl.class);

    private static final String BROKER_LOG_DIR = "getLogDir";

    @Autowired
    private TopicService topicService;

    @Autowired
    private KafkaAdminClient kafkaAdminClient;

    @Autowired
    private KafkaJMXClient kafkaJMXClient;

    @Autowired
    private JmxDAO jmxDAO;

    @Autowired
    private BrokerDAO brokerDAO;

    @Autowired
    private KafkaZKDAO kafkaZKDAO;

    @Override
    protected VersionItemTypeEnum getVersionItemType() {
        return SERVICE_SEARCH_BROKER;
    }

    private static final Cache<String, String> brokerVersionCache = Caffeine.newBuilder()
            .expireAfterWrite(1, TimeUnit.DAYS)
            .maximumSize(5000)
            .build();


    private final Cache<Long, List<Broker>> brokersCache = Caffeine.newBuilder()
            .expireAfterWrite(90, TimeUnit.SECONDS)
            .maximumSize(200)
            .build();

    @PostConstruct
    private void init() {
        registerVCHandler(BROKER_LOG_DIR,     V_1_0_0, V_MAX, "getLogDirByKafkaClient",          this::getLogDirByKafkaClient);
    }

    @Override
    public Result<List<Broker>> listBrokersFromKafka(ClusterPhy clusterPhy) {
        if (clusterPhy.getRunState().equals(ClusterRunStateEnum.RUN_ZK.getRunState())) {
            return this.getBrokersFromZKClient(clusterPhy);
        }

        return this.getBrokersFromAdminClient(clusterPhy);
    }

    @Override
    public void updateAliveBrokers(Long clusterPhyId, List<Broker> presentAliveBrokerList) {
        long now = System.currentTimeMillis();

        Map<Integer, Broker> presentAliveMap = presentAliveBrokerList.stream().collect(Collectors.toMap(Broker::getBrokerId, Function.identity()));

        List<BrokerPO> inDBBrokerPOList = this.getAllBrokerPOsFromDB(clusterPhyId);
        for (BrokerPO inDBBrokerPO: inDBBrokerPOList) {
            Broker presentAliveBroker = presentAliveMap.remove(inDBBrokerPO.getBrokerId());
            if (presentAliveBroker == null && Constant.DOWN.equals(inDBBrokerPO.getStatus())) {
                continue;
            } else if (presentAliveBroker == null && Constant.ALIVE.equals(inDBBrokerPO.getStatus())) {
                // 当前Broker已经挂了，但是DB中显示其存活，则将其状态设置为0
                inDBBrokerPO.setStatus(Constant.DOWN);
                inDBBrokerPO.setStartTimestamp(now); // 设置挂掉时间
                brokerDAO.updateById(inDBBrokerPO);
                continue;
            }

            // 如果当前Broker还存活，则更新DB信息
            BrokerPO newBrokerPO = ConvertUtil.obj2Obj(presentAliveBroker, BrokerPO.class);
            if (presentAliveBroker.getEndpointMap() != null) {
                newBrokerPO.setEndpointMap(ConvertUtil.obj2Json(presentAliveBroker.getEndpointMap()));
            }
            newBrokerPO.setId(inDBBrokerPO.getId());
            newBrokerPO.setStatus(Constant.ALIVE);
            newBrokerPO.setCreateTime(inDBBrokerPO.getCreateTime());
            newBrokerPO.setUpdateTime(new Date());
            if (newBrokerPO.getStartTimestamp() == null) {
                // 如果当前broker获取不到启动时间
                // 如果DB中的broker状态为down，则使用当前时间，否则使用db中已有broker的时间
                newBrokerPO.setStartTimestamp(inDBBrokerPO.getStatus().equals(0)? now: inDBBrokerPO.getStartTimestamp());
            }
            brokerDAO.updateById(newBrokerPO);
        }

        // 将presentAliveMap中剩下的Broker插入到DB中
        for (Broker presentAliveBroker: presentAliveMap.values()) {
            try {
                if (presentAliveBroker.getStartTimestamp() == null) {
                    presentAliveBroker.setStartTimestamp(now);
                }
                brokerDAO.insert(ConvertUtil.obj2Obj(presentAliveBroker, BrokerPO.class));
            } catch (DuplicateKeyException dke) {
                // 因为有多台KM一起执行，因此可能存在key冲突的问题，如果出现则直接忽略该错误
            }
        }
    }

    @Override
    public List<Broker> listAliveBrokersFromDB(Long clusterPhyId) {
        return this.listAllBrokersAndUpdateCache(clusterPhyId).stream().filter( elem -> elem.alive()).collect(Collectors.toList());
    }

    @Override
    public List<Broker> listAliveBrokersFromCacheFirst(Long clusterPhyId) {
        List<Broker> allBrokerList = brokersCache.getIfPresent(clusterPhyId);
        if (allBrokerList == null) {
            allBrokerList = this.listAllBrokersAndUpdateCache(clusterPhyId);
        }

        return allBrokerList.stream().filter(elem -> elem.alive()).collect(Collectors.toList());
    }

    @Override
    public List<Broker> listNotAliveBrokersFromDB(Long clusterPhyId) {
        return this.listAllBrokersAndUpdateCache(clusterPhyId).stream().filter( elem -> !elem.alive()).collect(Collectors.toList());
    }

    /**
     * 清理对应集群中下线的broker记录
     * @param clusterPhyId
     */
    @Override
    public void clearInactiveClusterPhyBrokers(Long clusterPhyId) {
        try {
            this.getAllBrokerPOsFromDB(clusterPhyId).stream()
                .filter(elem -> elem.getStatus().equals(Constant.DOWN))
                .forEach(elem -> brokerDAO.deleteById(elem.getId()));
        } catch (Exception e) {
            log.error("method=clearInactiveClusterPhyBrokers||clusterPhyId={}||errMsg=exception!", clusterPhyId, e);
        }
    }
    @Override
    public List<Broker> listAllBrokersFromDB(Long clusterPhyId) {
        return this.listAllBrokersAndUpdateCache(clusterPhyId);
    }

    @Override
    public List<Broker> listAllBrokerByTopic(Long clusterPhyId, String topicName) {
        List<Broker> brokerList = this.listAllBrokersFromDB(clusterPhyId);
        try {
            Topic topic = topicService.getTopic(clusterPhyId, topicName);
            if (topic == null) {
                return brokerList;
            }

            return brokerList.stream().filter(elem -> topic.getBrokerIdSet().contains(elem.getBrokerId())).collect(Collectors.toList());
        } catch (Exception e) {
            log.error("method=listAllBrokerByTopic||clusterPhyId={}||topicName={}||errMsg=exception!", clusterPhyId, topicName, e);
        }

        return brokerList;
    }

    @Override
    public Broker getBroker(Long clusterPhyId, Integer brokerId) {
        LambdaQueryWrapper<BrokerPO> lambdaQueryWrapper = new LambdaQueryWrapper<>();
        lambdaQueryWrapper.eq(BrokerPO::getClusterPhyId, clusterPhyId);
        lambdaQueryWrapper.eq(BrokerPO::getBrokerId, brokerId);

        return Broker.buildFrom(brokerDAO.selectOne(lambdaQueryWrapper));
    }

    @Override
    public Broker getBrokerFromCacheFirst(Long clusterPhyId, Integer brokerId) {
        List<Broker> brokerList = this.listAliveBrokersFromCacheFirst(clusterPhyId);
        if (brokerList == null) {
            return null;
        }

        for (Broker broker: brokerList) {
            if (brokerId.equals(broker.getBrokerId())) {
                return broker;
            }
        }

        return null;
    }

    @Override
    public Result<Map<String, LogDirDescription>> getBrokerLogDirDescFromKafka(Long clusterPhyId, Integer brokerId) {
        try {
            return (Result<Map<String, LogDirDescription>>) doVCHandler(clusterPhyId, BROKER_LOG_DIR, new BrokerParam(clusterPhyId, brokerId));
        } catch (VCHandlerNotExistException e) {
            return Result.buildFailure(VC_HANDLE_NOT_EXIST);
        }
    }

    @Override
    public String getBrokerVersionFromKafka(Long clusterId, Integer brokerId) {
        JmxConnectorWrap jmxConnectorWrap = kafkaJMXClient.getClientWithCheck(clusterId, brokerId);
        if (jmxConnectorWrap == null) {
            return "";
        }
        try {
            return (String) jmxConnectorWrap.getAttribute(new ObjectName(JMX_SERVER_APP_INFO + ",id=" + brokerId), VERSION);
        } catch (Exception e) {
            log.error("method=collectBrokerVersionFromKafka||clusterId:{}||brokerId:{}||errMsg=exception.", clusterId, brokerId, e);
        }

        return "";
    }

    @Override
    public String getBrokerVersionFromKafkaWithCacheFirst(Long clusterPhyId, Integer brokerId,Long startTime) {
        //id唯一确定一个broker
        String id = String.valueOf(clusterPhyId) + String.valueOf(brokerId)+String.valueOf(startTime);
        //先尝试读缓存
        String brokerVersion = brokerVersionCache.getIfPresent(id);
        if (brokerVersion != null) {
            return brokerVersion;
        }
        String version = getBrokerVersionFromKafka(clusterPhyId, brokerId);
        brokerVersionCache.put(id,version);
        return version;
    }

    @Override
    public Integer countAllBrokers() {
        LambdaQueryWrapper<BrokerPO> lambdaQueryWrapper = new LambdaQueryWrapper<>();
        return brokerDAO.selectCount(lambdaQueryWrapper);
    }

    @Override
    public boolean allServerDown(Long clusterPhyId) {
        List<BrokerPO> poList = this.getAllBrokerPOsFromDB(clusterPhyId);
        if (ValidateUtils.isEmptyList(poList)) {
            return false;
        }

        return poList.stream().filter(elem -> elem.getStatus().equals(Constant.DOWN)).count() == poList.size();
    }

    @Override
    public boolean existServerDown(Long clusterPhyId) {
        List<BrokerPO> poList = this.getAllBrokerPOsFromDB(clusterPhyId);
        if (ValidateUtils.isEmptyList(poList)) {
            return false;
        }

        return poList.stream().filter(elem -> elem.getStatus().equals(Constant.DOWN)).count() > 0;
    }

    /**************************************************** private method ****************************************************/

    private List<Broker> listAllBrokersAndUpdateCache(Long clusterPhyId) {
        List<Broker> allBrokerList = getAllBrokerPOsFromDB(clusterPhyId).stream().map(elem -> Broker.buildFrom(elem)).collect(Collectors.toList());
        brokersCache.put(clusterPhyId, allBrokerList);
        return allBrokerList;
    }

    private List<BrokerPO> getAllBrokerPOsFromDB(Long clusterPhyId) {
        LambdaQueryWrapper<BrokerPO> lambdaQueryWrapper = new LambdaQueryWrapper<>();
        lambdaQueryWrapper.eq(BrokerPO::getClusterPhyId, clusterPhyId);

        return brokerDAO.selectList(lambdaQueryWrapper);
    }

    private Result<Map<String, LogDirDescription>> getLogDirByKafkaClient(VersionItemParam itemParam) {
        BrokerParam brokerParam = (BrokerParam)itemParam;
        try {
            AdminClient adminClient = kafkaAdminClient.getClient(brokerParam.getClusterPhyId());

            DescribeLogDirsResult describeLogDirsResult = adminClient.describeLogDirs(Arrays.asList(brokerParam.getBrokerId()), new DescribeLogDirsOptions().timeoutMs(KafkaConstant.ADMIN_CLIENT_REQUEST_TIME_OUT_UNIT_MS));

            return Result.buildSuc(describeLogDirsResult.allDescriptions().get().get(brokerParam.getBrokerId()));
        } catch (NotExistException nee) {
            return Result.buildFromRSAndMsg(ResultStatus.NOT_EXIST, MsgConstant.getBrokerNotExist(brokerParam.getClusterPhyId(), brokerParam.getBrokerId()));
        } catch (Exception e) {
            log.error("");

            return Result.buildFromRSAndMsg(ResultStatus.KAFKA_OPERATE_FAILED, e.getMessage());
        }
    }

    private Result<List<Broker>> getBrokersFromZKClient(ClusterPhy clusterPhy) {
        try {
            List<Broker> brokerList = new ArrayList<>();

            List<String> brokerIdList = kafkaZKDAO.getChildren(clusterPhy.getId(), BrokerIdsZNode.path(), false);
            for (String brokerId: brokerIdList) {
                brokerList.add(kafkaZKDAO.getBrokerMetadata(clusterPhy.getId(), Integer.valueOf(brokerId)));
            }

            return Result.buildSuc(brokerList);
        } catch (Exception e) {
            log.error("method=getBrokersFromZKClient||clusterPhyId={}||errMsg=exception", clusterPhy.getId(), e);

            return Result.buildFromRSAndMsg(ResultStatus.ZK_OPERATE_FAILED, e.getMessage());
        }
    }

    private Result<List<Broker>> getBrokersFromAdminClient(ClusterPhy clusterPhy) {
        try {
            AdminClient adminClient = kafkaAdminClient.getClient(clusterPhy.getId());

            DescribeClusterResult describeClusterResult = adminClient.describeCluster(new DescribeClusterOptions().timeoutMs(KafkaConstant.ADMIN_CLIENT_REQUEST_TIME_OUT_UNIT_MS));

            JmxConfig jmxConfig = ConvertUtil.str2ObjByJson(clusterPhy.getJmxProperties(), JmxConfig.class);

            // 当前存活的Broker列表
            List<Broker> newBrokerList = new ArrayList<>();
            for (Node node: describeClusterResult.nodes().get()) {
                newBrokerList.add(this.getStartTimeAndBuildBroker(clusterPhy.getId(), node, jmxConfig));
            }

            return Result.buildSuc(newBrokerList);
        } catch (Exception e) {
            log.error("method=getBrokersFromAdminClient||clusterPhyId={}||errMsg=exception", clusterPhy.getId(), e);

            return Result.buildFromRSAndMsg(ResultStatus.KAFKA_OPERATE_FAILED, e.getMessage());
        }
    }

    private Broker getStartTimeAndBuildBroker(Long clusterPhyId, Node newNode, JmxConfig jmxConfig) {
        try {
            Long startTime = jmxDAO.getServerStartTime(clusterPhyId, newNode.host(), jmxConfig.getFinallyJmxPort(String.valueOf(newNode.id())), jmxConfig);

            return Broker.buildFrom(clusterPhyId, newNode, startTime);
        } catch (Exception e) {
            log.error("method=getStartTimeAndBuildBroker||clusterPhyId={}||brokerNode={}||jmxConfig={}||errMsg=exception!", clusterPhyId, newNode, jmxConfig, e);
        }

        return Broker.buildFrom(clusterPhyId, newNode, null);
    }
}
