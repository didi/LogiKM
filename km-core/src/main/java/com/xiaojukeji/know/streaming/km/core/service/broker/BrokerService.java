package com.xiaojukeji.know.streaming.km.core.service.broker;

import com.xiaojukeji.know.streaming.km.common.bean.entity.broker.Broker;
import com.xiaojukeji.know.streaming.km.common.bean.entity.cluster.ClusterPhy;
import com.xiaojukeji.know.streaming.km.common.bean.entity.result.Result;
import org.apache.kafka.clients.admin.LogDirDescription;

import java.util.List;
import java.util.Map;

public interface BrokerService {
    /**
     * 从Kafka获取存活的Broker列表
     */
    Result<List<Broker>> listBrokersFromKafka(ClusterPhy clusterPhy);

    /**
     * 更新存活的Broker列表
     */
    void updateAliveBrokers(Long clusterPhyId, List<Broker> presentAliveBrokerList);

    /**
     * 从DB获取存活的Broker列表
     */
    List<Broker> listAliveBrokersFromDB(Long clusterPhyId);
    List<Broker> listAliveBrokersFromCacheFirst(Long clusterPhyId);

    /**
     * 从DB获取未存活的Broker列表
     */
    List<Broker> listNotAliveBrokersFromDB(Long clusterPhyId);

    /**
     * 从DB获取所有Broker列表
     */
    List<Broker> listAllBrokersFromDB(Long clusterPhyId);

    /**
     * 获取Topic下的Broker列表
     */
    List<Broker> listAllBrokerByTopic(Long clusterPhyId, String topicName);

    /**
     * 获取具体Broker
     */
    Broker getBroker(Long clusterPhyId, Integer brokerId);
    Broker getBrokerFromCacheFirst(Long clusterPhyId, Integer brokerId);

    /**
     * 获取BrokerLog-Dir信息
     */
    Result<Map<String, LogDirDescription>> getBrokerLogDirDescFromKafka(Long clusterPhyId, Integer brokerId);

    /**
     * 获取Broker的版本信息
     */
    String getBrokerVersionFromKafka(Long clusterPhyId, Integer brokerId);

    /**
     * 优先从本地缓存中获取Broker的版本信息
     * @param
     * @return
     */
    String getBrokerVersionFromKafkaWithCacheFirst(Long clusterPhyId, Integer brokerId,Long startTime);

    /**
     * 获取总的Broker数
     */
    Integer countAllBrokers();

    boolean allServerDown(Long clusterPhyId);

    boolean existServerDown(Long clusterPhyId);

    void clearInactiveClusterPhyBrokers(Long clusterPhyId);
}
