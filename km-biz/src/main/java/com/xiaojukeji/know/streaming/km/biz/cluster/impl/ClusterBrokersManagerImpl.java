package com.xiaojukeji.know.streaming.km.biz.cluster.impl;

import com.didiglobal.logi.log.ILog;
import com.didiglobal.logi.log.LogFactory;
import com.xiaojukeji.know.streaming.km.biz.cluster.ClusterBrokersManager;
import com.xiaojukeji.know.streaming.km.common.bean.dto.cluster.ClusterBrokersOverviewDTO;
import com.xiaojukeji.know.streaming.km.common.bean.dto.pagination.PaginationSortDTO;
import com.xiaojukeji.know.streaming.km.common.bean.entity.broker.Broker;
import com.xiaojukeji.know.streaming.km.common.bean.entity.cluster.ClusterPhy;
import com.xiaojukeji.know.streaming.km.common.bean.entity.config.JmxConfig;
import com.xiaojukeji.know.streaming.km.common.bean.entity.kafkacontroller.KafkaController;
import com.xiaojukeji.know.streaming.km.common.bean.entity.metrics.BrokerMetrics;
import com.xiaojukeji.know.streaming.km.common.bean.entity.result.PaginationResult;
import com.xiaojukeji.know.streaming.km.common.bean.entity.result.Result;
import com.xiaojukeji.know.streaming.km.common.bean.entity.topic.Topic;
import com.xiaojukeji.know.streaming.km.common.bean.vo.cluster.res.ClusterBrokersOverviewVO;
import com.xiaojukeji.know.streaming.km.common.bean.vo.cluster.res.ClusterBrokersStateVO;
import com.xiaojukeji.know.streaming.km.common.bean.vo.kafkacontroller.KafkaControllerVO;
import com.xiaojukeji.know.streaming.km.common.constant.KafkaConstant;
import com.xiaojukeji.know.streaming.km.common.enums.SortTypeEnum;
import com.xiaojukeji.know.streaming.km.common.enums.cluster.ClusterRunStateEnum;
import com.xiaojukeji.know.streaming.km.common.utils.ConvertUtil;
import com.xiaojukeji.know.streaming.km.common.utils.PaginationMetricsUtil;
import com.xiaojukeji.know.streaming.km.common.utils.PaginationUtil;
import com.xiaojukeji.know.streaming.km.common.utils.ValidateUtils;
import com.xiaojukeji.know.streaming.km.core.service.broker.BrokerConfigService;
import com.xiaojukeji.know.streaming.km.core.service.broker.BrokerMetricService;
import com.xiaojukeji.know.streaming.km.core.service.broker.BrokerService;
import com.xiaojukeji.know.streaming.km.core.service.kafkacontroller.KafkaControllerService;
import com.xiaojukeji.know.streaming.km.core.service.topic.TopicService;
import com.xiaojukeji.know.streaming.km.persistence.cache.LoadedClusterPhyCache;
import com.xiaojukeji.know.streaming.km.persistence.kafka.KafkaJMXClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;


@Service
public class ClusterBrokersManagerImpl implements ClusterBrokersManager {
    private static final ILog log = LogFactory.getLog(ClusterBrokersManagerImpl.class);

    @Autowired
    private TopicService topicService;

    @Autowired
    private BrokerService brokerService;

    @Autowired
    private BrokerConfigService brokerConfigService;

    @Autowired
    private BrokerMetricService brokerMetricService;

    @Autowired
    private KafkaControllerService kafkaControllerService;

    @Autowired
    private KafkaJMXClient kafkaJMXClient;

    @Override
    public PaginationResult<ClusterBrokersOverviewVO> getClusterPhyBrokersOverview(Long clusterPhyId, ClusterBrokersOverviewDTO dto) {
        // 获取集群Broker列表
        List<Broker> brokerList = brokerService.listAllBrokersFromDB(clusterPhyId);

        // 搜索
        brokerList = PaginationUtil.pageByFuzzyFilter(brokerList, dto.getSearchKeywords(), Arrays.asList("host"));

        // 获取指标
        Result<List<BrokerMetrics>> metricsResult = brokerMetricService.getLatestMetricsFromES(
                clusterPhyId,
                brokerList.stream().filter(elem1 -> elem1.alive()).map(elem2 -> elem2.getBrokerId()).collect(Collectors.toList())
        );

        // 分页 + 搜索
        PaginationResult<Integer> paginationResult = this.pagingBrokers(brokerList, metricsResult.hasData()? metricsResult.getData(): new ArrayList<>(), dto);

        // 获取__consumer_offsetsTopic的分布
        Topic groupTopic = topicService.getTopic(clusterPhyId, org.apache.kafka.common.internals.Topic.GROUP_METADATA_TOPIC_NAME);
        Topic transactionTopic = topicService.getTopic(clusterPhyId, org.apache.kafka.common.internals.Topic.TRANSACTION_STATE_TOPIC_NAME);

        //获取controller信息
        KafkaController kafkaController = kafkaControllerService.getKafkaControllerFromDB(clusterPhyId);

        //获取jmx状态信息
        Map<Integer, Boolean> jmxConnectedMap = new HashMap<>();
        brokerList.forEach(elem -> jmxConnectedMap.put(elem.getBrokerId(), kafkaJMXClient.getClientWithCheck(clusterPhyId, elem.getBrokerId()) != null));


        ClusterPhy clusterPhy = LoadedClusterPhyCache.getByPhyId(clusterPhyId);

        // 格式转换
        return PaginationResult.buildSuc(
                this.convert2ClusterBrokersOverviewVOList(
                        clusterPhy,
                        paginationResult.getData().getBizData(),
                        brokerList,
                        metricsResult.getData(),
                        groupTopic,
                        transactionTopic,
                        kafkaController,
                        jmxConnectedMap
                ),
                paginationResult
        );
    }

    @Override
    public PaginationResult<ClusterBrokersOverviewVO> clearInactiveClusterPhyBrokers(Long clusterPhyId, ClusterBrokersOverviewDTO dto) {
        brokerService.clearInactiveClusterPhyBrokers(clusterPhyId);
        return this.getClusterPhyBrokersOverview(clusterPhyId, dto);
    }

    @Override
    public ClusterBrokersStateVO getClusterPhyBrokersState(Long clusterPhyId) {
        ClusterBrokersStateVO clusterBrokersStateVO = new ClusterBrokersStateVO();

        // 获取集群Broker列表
        List<Broker> allBrokerList = brokerService.listAllBrokersFromDB(clusterPhyId);
        if (allBrokerList == null) {
            allBrokerList = new ArrayList<>();
        }

        // 设置broker数
        clusterBrokersStateVO.setBrokerCount(allBrokerList.size());

        // 设置版本信息
        clusterBrokersStateVO.setBrokerVersionList(
                this.getBrokerVersionList(clusterPhyId, allBrokerList.stream().filter(elem -> elem.alive()).collect(Collectors.toList()))
        );

        // 获取controller信息
        KafkaController kafkaController = kafkaControllerService.getKafkaControllerFromDB(clusterPhyId);

        // 设置kafka-controller信息
        clusterBrokersStateVO.setKafkaControllerAlive(false);
        if(null != kafkaController) {
            clusterBrokersStateVO.setKafkaController(
                    this.convert2KafkaControllerVO(
                            kafkaController,
                            brokerService.getBroker(clusterPhyId, kafkaController.getBrokerId())
                    )
            );
            clusterBrokersStateVO.setKafkaControllerAlive(true);
        }

        clusterBrokersStateVO.setConfigSimilar(brokerConfigService.countBrokerConfigDiffsFromDB(clusterPhyId, KafkaConstant.CONFIG_SIMILAR_IGNORED_CONFIG_KEY_LIST) <= 0
        );

        return clusterBrokersStateVO;
    }

    /**************************************************** private method ****************************************************/

    private PaginationResult<Integer> pagingBrokers(List<Broker> brokerList, List<BrokerMetrics> metricsList, PaginationSortDTO dto) {
        if (ValidateUtils.isBlank(dto.getSortField())) {
            // 默认排序
            return PaginationUtil.pageBySubData(
                    PaginationUtil.pageBySort(brokerList, "brokerId", SortTypeEnum.ASC.getSortType()).stream().map(elem -> elem.getBrokerId()).collect(Collectors.toList()),
                    dto
            );
        }
        if (!brokerMetricService.isMetricName(dto.getSortField())) {
            // 非指标字段进行排序，分页
            return PaginationUtil.pageBySubData(
                    PaginationUtil.pageBySort(brokerList, dto.getSortField(), dto.getSortType()).stream().map(elem -> elem.getBrokerId()).collect(Collectors.toList()),
                    dto
            );
        }

        // 指标字段进行排序及分页
        Map<Integer, BrokerMetrics> metricsMap = metricsList.stream().collect(Collectors.toMap(BrokerMetrics::getBrokerId, Function.identity()));
        brokerList.stream().forEach(elem -> {
            metricsMap.putIfAbsent(elem.getBrokerId(), new BrokerMetrics(elem.getClusterPhyId(), elem.getBrokerId()));
        });

        // 排序
        metricsList = (List<BrokerMetrics>) PaginationMetricsUtil.sortMetrics(new ArrayList<>(metricsMap.values()), dto.getSortField(), "brokerId", dto.getSortType());

        return PaginationUtil.pageBySubData(
                metricsList.stream().map(elem -> elem.getBrokerId()).collect(Collectors.toList()),
                dto
        );
    }

    private List<ClusterBrokersOverviewVO> convert2ClusterBrokersOverviewVOList(ClusterPhy clusterPhy,
                                                                                List<Integer> pagedBrokerIdList,
                                                                                List<Broker> brokerList,
                                                                                List<BrokerMetrics> metricsList,
                                                                                Topic groupTopic,
                                                                                Topic transactionTopic,
                                                                                KafkaController kafkaController,
                                                                                Map<Integer, Boolean> jmxConnectedMap) {
        Map<Integer, BrokerMetrics> metricsMap = metricsList == null ? new HashMap<>() : metricsList.stream().collect(Collectors.toMap(BrokerMetrics::getBrokerId, Function.identity()));

        Map<Integer, Broker> brokerMap = brokerList == null ? new HashMap<>() : brokerList.stream().collect(Collectors.toMap(Broker::getBrokerId, Function.identity()));

        List<ClusterBrokersOverviewVO> voList = new ArrayList<>(pagedBrokerIdList.size());
        for (Integer brokerId : pagedBrokerIdList) {
            Broker broker = brokerMap.get(brokerId);
            BrokerMetrics brokerMetrics = metricsMap.get(brokerId);
            Boolean jmxConnected = jmxConnectedMap.get(brokerId);
            voList.add(this.convert2ClusterBrokersOverviewVO(brokerId, broker, brokerMetrics, groupTopic, transactionTopic, kafkaController, jmxConnected));
        }

        //补充非zk模式的JMXPort信息
        if (!clusterPhy.getRunState().equals(ClusterRunStateEnum.RUN_ZK.getRunState())) {
            JmxConfig jmxConfig = ConvertUtil.str2ObjByJson(clusterPhy.getJmxProperties(), JmxConfig.class);
            voList.forEach(elem -> elem.setJmxPort(jmxConfig.getFinallyJmxPort(String.valueOf(elem.getBrokerId()))));
        }

        return voList;
    }

    private ClusterBrokersOverviewVO convert2ClusterBrokersOverviewVO(Integer brokerId, Broker broker, BrokerMetrics brokerMetrics, Topic groupTopic, Topic transactionTopic, KafkaController kafkaController, Boolean jmxConnected) {
        ClusterBrokersOverviewVO clusterBrokersOverviewVO = new ClusterBrokersOverviewVO();
        clusterBrokersOverviewVO.setBrokerId(brokerId);
        if (broker != null) {
            clusterBrokersOverviewVO.setHost(broker.getHost());
            clusterBrokersOverviewVO.setRack(broker.getRack());
            clusterBrokersOverviewVO.setJmxPort(broker.getJmxPort());
            clusterBrokersOverviewVO.setAlive(broker.alive());
            clusterBrokersOverviewVO.setStartTimeUnitMs(broker.getStartTimestamp());
        }
        clusterBrokersOverviewVO.setKafkaRoleList(new ArrayList<>());

        if (groupTopic != null && groupTopic.getBrokerIdSet().contains(brokerId)) {
            clusterBrokersOverviewVO.getKafkaRoleList().add(groupTopic.getTopicName());
        }
        if (transactionTopic != null && transactionTopic.getBrokerIdSet().contains(brokerId)) {
            clusterBrokersOverviewVO.getKafkaRoleList().add(transactionTopic.getTopicName());
        }
        if (kafkaController != null && kafkaController.getBrokerId().equals(brokerId)) {
            clusterBrokersOverviewVO.getKafkaRoleList().add(KafkaConstant.CONTROLLER_ROLE);
        }

        clusterBrokersOverviewVO.setLatestMetrics(brokerMetrics);
        clusterBrokersOverviewVO.setJmxConnected(jmxConnected);
        return clusterBrokersOverviewVO;
    }

    private KafkaControllerVO convert2KafkaControllerVO(KafkaController kafkaController, Broker kafkaControllerBroker) {
        if(null != kafkaController && null != kafkaControllerBroker) {
            KafkaControllerVO kafkaControllerVO = new KafkaControllerVO();
            kafkaControllerVO.setBrokerId(kafkaController.getBrokerId());
            kafkaControllerVO.setBrokerHost(kafkaControllerBroker.getHost());
            return kafkaControllerVO;
        }

        return null;
    }

    private List<String> getBrokerVersionList(Long clusterPhyId, List<Broker> brokerList) {
        Set<String> brokerVersionList = new HashSet<>();
        for (Broker broker : brokerList) {
            brokerVersionList.add(brokerService.getBrokerVersionFromKafkaWithCacheFirst(broker.getClusterPhyId(),broker.getBrokerId(),broker.getStartTimestamp()));
        }
        brokerVersionList.remove("");
        return new ArrayList<>(brokerVersionList);
    }
}
