package com.xiaojukeji.know.streaming.km.biz.cluster;

import com.xiaojukeji.know.streaming.km.common.bean.dto.cluster.ClusterBrokersOverviewDTO;
import com.xiaojukeji.know.streaming.km.common.bean.entity.result.PaginationResult;
import com.xiaojukeji.know.streaming.km.common.bean.vo.cluster.res.ClusterBrokersOverviewVO;
import com.xiaojukeji.know.streaming.km.common.bean.vo.cluster.res.ClusterBrokersStateVO;

/**
 * 多集群总体状态
 */
public interface ClusterBrokersManager {
    /**
     * 获取缓存查询结果 & broker 表查询结果并集
     * @param clusterPhyId kafka 物理集群 id
     * @param dto 封装分页查询参数对象
     * @return 返回获取到的缓存查询结果 & broker 表查询结果并集
     */
    PaginationResult<ClusterBrokersOverviewVO> getClusterPhyBrokersOverview(Long clusterPhyId, ClusterBrokersOverviewDTO dto);

    /**
     * 删除status == 0 的所有broker -> 获取缓存查询结果 & broker 表查询结果并集
     * 获取缓存查询结果 & broker 表查询结果并集
     * @param clusterPhyId kafka 物理集群 id
     * @param dto 封装分页查询参数对象
     * @return 返回获取到的缓存查询结果 & broker 表查询结果并集
     */
    PaginationResult<ClusterBrokersOverviewVO> clearInactiveClusterPhyBrokers(Long clusterPhyId, ClusterBrokersOverviewDTO dto);


    /**
     * 根据物理集群id获取集群对应broker状态信息
     * @param clusterPhyId 物理集群 id
     * @return 返回根据物理集群id获取到的集群对应broker状态信息
     */
    ClusterBrokersStateVO getClusterPhyBrokersState(Long clusterPhyId);
}
