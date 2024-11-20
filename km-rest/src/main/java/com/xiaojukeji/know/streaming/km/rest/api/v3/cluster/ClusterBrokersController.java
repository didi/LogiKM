package com.xiaojukeji.know.streaming.km.rest.api.v3.cluster;

import com.xiaojukeji.know.streaming.km.biz.cluster.ClusterBrokersManager;
import com.xiaojukeji.know.streaming.km.common.bean.dto.cluster.ClusterBrokersOverviewDTO;
import com.xiaojukeji.know.streaming.km.common.bean.entity.broker.Broker;
import com.xiaojukeji.know.streaming.km.common.bean.entity.result.PaginationResult;
import com.xiaojukeji.know.streaming.km.common.bean.entity.result.Result;
import com.xiaojukeji.know.streaming.km.common.bean.vo.cluster.res.ClusterBrokersOverviewVO;
import com.xiaojukeji.know.streaming.km.common.bean.vo.cluster.res.ClusterBrokersStateVO;
import com.xiaojukeji.know.streaming.km.common.bean.vo.metadata.BrokerMetadataVO;
import com.xiaojukeji.know.streaming.km.common.constant.ApiPrefix;
import com.xiaojukeji.know.streaming.km.common.constant.Constant;
import com.xiaojukeji.know.streaming.km.common.utils.ConvertUtil;
import com.xiaojukeji.know.streaming.km.common.utils.PaginationUtil;
import com.xiaojukeji.know.streaming.km.core.service.broker.BrokerService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Arrays;
import java.util.List;


/**
 * @author zengqiao
 * @date 22/02/21
 */
@Api(tags = Constant.SWAGGER_API_TAG_PREFIX + "集群Brokers-相关接口(REST)")
@RestController
@RequestMapping(ApiPrefix.API_V3_PREFIX)
public class ClusterBrokersController {

    @Autowired
    private BrokerService brokerService;

    @Autowired
    private ClusterBrokersManager clusterBrokersManager;

    @ApiOperation(value = "集群Brokers元信息")
    @GetMapping(value = "clusters/{clusterPhyId}/brokers-metadata")
    @ResponseBody
    public Result<List<BrokerMetadataVO>> getClusterPhyBrokersMetadata(@PathVariable Long clusterPhyId,
                                                                       @RequestParam(required = false) String searchKeyword) {
        List<Broker> brokerList = brokerService.listAllBrokersFromDB(clusterPhyId);
        return Result.buildSuc(
                PaginationUtil.pageByFuzzyFilter(ConvertUtil.list2List(brokerList, BrokerMetadataVO.class), searchKeyword, Arrays.asList("host"))
        );
    }

    @ApiOperation(value = "集群brokers状态信息")
    @GetMapping(value = "clusters/{clusterPhyId}/brokers-state")
    @ResponseBody
    public Result<ClusterBrokersStateVO> getClusterPhyBrokersState(@PathVariable Long clusterPhyId) {
        return Result.buildSuc(clusterBrokersManager.getClusterPhyBrokersState(clusterPhyId));
    }

    @ApiOperation(value = "集群brokers信息列表")
    @PostMapping(value = "clusters/{clusterPhyId}/brokers-overview")
    @ResponseBody
    public PaginationResult<ClusterBrokersOverviewVO> getClusterPhyBrokersOverview(@PathVariable Long clusterPhyId,
                                                                                   @RequestBody ClusterBrokersOverviewDTO dto) {
        return clusterBrokersManager.getClusterPhyBrokersOverview(clusterPhyId, dto);
    }

    @ApiOperation(value = "集群无效brokers清理")
    @PostMapping(value = "clusters/{clusterPhyId}/brokers-clear")
    @ResponseBody
    public PaginationResult<ClusterBrokersOverviewVO> clearInactiveClusterPhyBrokers(@PathVariable Long clusterPhyId,
                                                                                   @RequestBody ClusterBrokersOverviewDTO dto) {
        return clusterBrokersManager.clearInactiveClusterPhyBrokers(clusterPhyId, dto);
    }
}