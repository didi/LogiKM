package com.xiaojukeji.know.streaming.km.biz.group.impl;

import com.didiglobal.logi.log.ILog;
import com.didiglobal.logi.log.LogFactory;
import com.xiaojukeji.know.streaming.km.biz.group.GroupManager;
import com.xiaojukeji.know.streaming.km.common.bean.dto.cluster.ClusterGroupSummaryDTO;
import com.xiaojukeji.know.streaming.km.common.bean.dto.group.GroupOffsetDeleteDTO;
import com.xiaojukeji.know.streaming.km.common.bean.dto.group.GroupOffsetResetDTO;
import com.xiaojukeji.know.streaming.km.common.bean.dto.pagination.PaginationBaseDTO;
import com.xiaojukeji.know.streaming.km.common.bean.dto.pagination.PaginationSortDTO;
import com.xiaojukeji.know.streaming.km.common.bean.dto.partition.PartitionOffsetDTO;
import com.xiaojukeji.know.streaming.km.common.bean.entity.cluster.ClusterPhy;
import com.xiaojukeji.know.streaming.km.common.bean.entity.group.Group;
import com.xiaojukeji.know.streaming.km.common.bean.entity.group.GroupTopic;
import com.xiaojukeji.know.streaming.km.common.bean.entity.group.GroupTopicMember;
import com.xiaojukeji.know.streaming.km.common.bean.entity.kafka.KSGroupDescription;
import com.xiaojukeji.know.streaming.km.common.bean.entity.kafka.KSMemberConsumerAssignment;
import com.xiaojukeji.know.streaming.km.common.bean.entity.kafka.KSMemberDescription;
import com.xiaojukeji.know.streaming.km.common.bean.entity.metrics.GroupMetrics;
import com.xiaojukeji.know.streaming.km.common.bean.entity.offset.KSOffsetSpec;
import com.xiaojukeji.know.streaming.km.common.bean.entity.param.group.DeleteGroupParam;
import com.xiaojukeji.know.streaming.km.common.bean.entity.param.group.DeleteGroupTopicParam;
import com.xiaojukeji.know.streaming.km.common.bean.entity.param.group.DeleteGroupTopicPartitionParam;
import com.xiaojukeji.know.streaming.km.common.bean.entity.result.PaginationResult;
import com.xiaojukeji.know.streaming.km.common.bean.entity.result.Result;
import com.xiaojukeji.know.streaming.km.common.bean.entity.topic.Topic;
import com.xiaojukeji.know.streaming.km.common.bean.entity.topic.TopicPartitionKS;
import com.xiaojukeji.know.streaming.km.common.bean.entity.result.ResultStatus;
import com.xiaojukeji.know.streaming.km.common.bean.po.group.GroupMemberPO;
import com.xiaojukeji.know.streaming.km.common.bean.vo.group.GroupOverviewVO;
import com.xiaojukeji.know.streaming.km.common.bean.vo.group.GroupTopicConsumedDetailVO;
import com.xiaojukeji.know.streaming.km.common.bean.vo.group.GroupTopicOverviewVO;
import com.xiaojukeji.know.streaming.km.common.constant.MsgConstant;
import com.xiaojukeji.know.streaming.km.common.constant.PaginationConstant;
import com.xiaojukeji.know.streaming.km.common.converter.GroupConverter;
import com.xiaojukeji.know.streaming.km.common.enums.AggTypeEnum;
import com.xiaojukeji.know.streaming.km.common.enums.OffsetTypeEnum;
import com.xiaojukeji.know.streaming.km.common.enums.SortTypeEnum;
import com.xiaojukeji.know.streaming.km.common.enums.group.DeleteGroupTypeEnum;
import com.xiaojukeji.know.streaming.km.common.enums.group.GroupStateEnum;
import com.xiaojukeji.know.streaming.km.common.exception.AdminOperateException;
import com.xiaojukeji.know.streaming.km.common.exception.NotExistException;
import com.xiaojukeji.know.streaming.km.common.utils.ConvertUtil;
import com.xiaojukeji.know.streaming.km.common.utils.PaginationMetricsUtil;
import com.xiaojukeji.know.streaming.km.common.utils.PaginationUtil;
import com.xiaojukeji.know.streaming.km.common.utils.ValidateUtils;
import com.xiaojukeji.know.streaming.km.core.service.cluster.ClusterPhyService;
import com.xiaojukeji.know.streaming.km.core.service.config.KSConfigUtils;
import com.xiaojukeji.know.streaming.km.core.service.group.GroupMetricService;
import com.xiaojukeji.know.streaming.km.core.service.group.GroupService;
import com.xiaojukeji.know.streaming.km.core.service.group.OpGroupService;
import com.xiaojukeji.know.streaming.km.core.service.partition.PartitionService;
import com.xiaojukeji.know.streaming.km.core.service.topic.TopicService;
import com.xiaojukeji.know.streaming.km.core.service.version.metrics.kafka.GroupMetricVersionItems;
import com.xiaojukeji.know.streaming.km.core.utils.ApiCallThreadPoolService;
import com.xiaojukeji.know.streaming.km.persistence.es.dao.GroupMetricESDAO;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.xiaojukeji.know.streaming.km.common.enums.group.GroupTypeEnum.CONNECT_CLUSTER_PROTOCOL_TYPE;

@Component
public class GroupManagerImpl implements GroupManager {
    private static final ILog LOGGER = LogFactory.getLog(GroupManagerImpl.class);

    @Autowired
    private TopicService topicService;

    @Autowired
    private GroupService groupService;

    @Autowired
    private OpGroupService opGroupService;

    @Autowired
    private PartitionService partitionService;

    @Autowired
    private GroupMetricService groupMetricService;

    @Autowired
    private GroupMetricESDAO groupMetricESDAO;

    @Autowired
    private ClusterPhyService clusterPhyService;

    @Autowired
    private KSConfigUtils ksConfigUtils;

    @Override
    public PaginationResult<GroupTopicOverviewVO> pagingGroupMembers(Long clusterPhyId,
                                                                     String topicName,
                                                                     String groupName,
                                                                     String searchTopicKeyword,
                                                                     String searchGroupKeyword,
                                                                     PaginationBaseDTO dto) {
        long startTimeUnitMs = System.currentTimeMillis();

        PaginationResult<GroupMemberPO> paginationResult = groupService.pagingGroupMembers(clusterPhyId, topicName, groupName, searchTopicKeyword, searchGroupKeyword, dto);

        if (!paginationResult.hasData()) {
            return PaginationResult.buildSuc(new ArrayList<>(), paginationResult);
        }

        List<GroupTopicOverviewVO> groupTopicVOList = this.getGroupTopicOverviewVOList(
                clusterPhyId,
                paginationResult.getData().getBizData(),
                ksConfigUtils.getApiCallLeftTimeUnitMs(System.currentTimeMillis() - startTimeUnitMs)    // 超时时间
        );

        return PaginationResult.buildSuc(groupTopicVOList, paginationResult);
    }

    @Override
    public PaginationResult<GroupTopicOverviewVO> pagingGroupTopicMembers(Long clusterPhyId, String groupName, PaginationBaseDTO dto) throws Exception {
        long startTimeUnitMs = System.currentTimeMillis();

        ClusterPhy clusterPhy = clusterPhyService.getClusterByCluster(clusterPhyId);
        if (clusterPhy == null) {
            return PaginationResult.buildFailure(MsgConstant.getClusterPhyNotExist(clusterPhyId), dto);
        }

        Group group = groupService.getGroupFromKafka(clusterPhy, groupName);

        //没有topicMember则直接返回
        if (group == null || ValidateUtils.isEmptyList(group.getTopicMembers())) {
            return PaginationResult.buildSuc(dto);
        }

        //排序
        List<GroupTopicMember> groupTopicMembers = PaginationUtil.pageBySort(group.getTopicMembers(), PaginationConstant.DEFAULT_GROUP_TOPIC_SORTED_FIELD, SortTypeEnum.DESC.getSortType());

        //分页
        PaginationResult<GroupTopicMember> paginationResult = PaginationUtil.pageBySubData(groupTopicMembers, dto);

        List<GroupMemberPO> groupMemberPOList = paginationResult.getData().getBizData().stream().map(elem -> new GroupMemberPO(clusterPhyId, elem.getTopicName(), groupName, group.getState().getState(), elem.getMemberCount())).collect(Collectors.toList());

        return PaginationResult.buildSuc(
                this.getGroupTopicOverviewVOList(
                        clusterPhyId,
                        groupMemberPOList,
                        ksConfigUtils.getApiCallLeftTimeUnitMs(System.currentTimeMillis() - startTimeUnitMs)    // 超时时间
                ),
                paginationResult
        );
    }

    @Override
    public PaginationResult<GroupOverviewVO> pagingClusterGroupsOverview(Long clusterPhyId, ClusterGroupSummaryDTO dto) {
        List<Group> groupList = groupService.listClusterGroups(clusterPhyId);

        // 类型转化
        List<GroupOverviewVO> voList = groupList.stream().map(GroupConverter::convert2GroupOverviewVO).collect(Collectors.toList());

        // 搜索groupName
        voList = PaginationUtil.pageByFuzzyFilter(voList, dto.getSearchGroupName(), Arrays.asList("name"));

        //搜索topic
        if (!ValidateUtils.isBlank(dto.getSearchTopicName())) {
            voList = voList.stream().filter(elem -> {
                for (String topicName : elem.getTopicNameList()) {
                    if (topicName.contains(dto.getSearchTopicName())) {
                        return true;
                    }
                }
                return false;
            }).collect(Collectors.toList());
        }

        // 分页 后 返回
        return PaginationUtil.pageBySubData( PaginationUtil.pageBySort(voList, dto.getSortField(), dto.getSortType(),"memberCount", dto.getSortType()), dto);
    }

    @Override
    public PaginationResult<GroupTopicConsumedDetailVO> pagingGroupTopicConsumedMetrics(Long clusterPhyId,
                                                                                        String topicName,
                                                                                        String groupName,
                                                                                        List<String> latestMetricNames,
                                                                                        PaginationSortDTO dto) throws NotExistException, AdminOperateException {
        ClusterPhy clusterPhy = clusterPhyService.getClusterByCluster(clusterPhyId);
        if (clusterPhy == null) {
            return PaginationResult.buildFailure(MsgConstant.getClusterPhyNotExist(clusterPhyId), dto);
        }

        // 获取消费组消费的TopicPartition列表
        Map<TopicPartition, Long> consumedOffsetMap = groupService.getGroupOffsetFromKafka(clusterPhyId, groupName);
        List<Integer> partitionList = consumedOffsetMap.keySet()
                .stream()
                .filter(elem -> elem.topic().equals(topicName))
                .map(elem -> elem.partition())
                .collect(Collectors.toList());
        Collections.sort(partitionList);

        // 获取消费组当前运行信息
        KSGroupDescription groupDescription = groupService.getGroupDescriptionFromKafka(clusterPhy, groupName);

        // 转换存储格式
        Map<TopicPartition, KSMemberDescription> tpMemberMap = new HashMap<>();

        // 如果不是connect集群
        if (!groupDescription.protocolType().equals(CONNECT_CLUSTER_PROTOCOL_TYPE)) {
            for (KSMemberDescription description : groupDescription.members()) {
                // 如果是 Consumer 的 Description ，则 Assignment 的类型为 KSMemberConsumerAssignment 的
                KSMemberConsumerAssignment assignment = (KSMemberConsumerAssignment) description.assignment();
                for (TopicPartition tp : assignment.topicPartitions()) {
                    tpMemberMap.put(tp, description);
                }
            }
        }

        // 获取指标
        PaginationResult<GroupMetrics> metricsResult = this.pagingGroupTopicPartitionMetrics(clusterPhyId, groupName, topicName, partitionList, latestMetricNames, dto);
        if (metricsResult.failed()) {
            return PaginationResult.buildFailure(metricsResult, dto);
        }

        // 数据组装
        List<GroupTopicConsumedDetailVO> voList = new ArrayList<>();
        for (GroupMetrics groupMetrics: metricsResult.getData().getBizData()) {
            GroupTopicConsumedDetailVO vo = new GroupTopicConsumedDetailVO();
            vo.setTopicName(topicName);
            vo.setPartitionId(groupMetrics.getPartitionId());

            KSMemberDescription ksMemberDescription = tpMemberMap.get(new TopicPartition(topicName, groupMetrics.getPartitionId()));
            if (ksMemberDescription != null) {
                vo.setMemberId(ksMemberDescription.consumerId());
                vo.setHost(ksMemberDescription.host());
                vo.setClientId(ksMemberDescription.clientId());
            }

            vo.setLatestMetrics(groupMetrics);
            voList.add(vo);
        }

        return PaginationResult.buildSuc(voList, metricsResult);
    }

    @Override
    public Result<Set<TopicPartitionKS>> listClusterPhyGroupPartitions(Long clusterPhyId, String groupName, Long startTime, Long endTime) {
        try {
            return Result.buildSuc(groupMetricESDAO.listGroupTopicPartitions(clusterPhyId, groupName, startTime, endTime));
        }catch (Exception e){
            return Result.buildFailure(e.getMessage());
        }
    }

    @Override
    public Result<Void> resetGroupOffsets(GroupOffsetResetDTO dto, String operator) throws Exception {
        Result<Void> rv = this.checkFieldLegal(dto);
        if (rv.failed()) {
            return rv;
        }

        ClusterPhy clusterPhy = clusterPhyService.getClusterByCluster(dto.getClusterId());
        if (clusterPhy == null) {
            return Result.buildFromRSAndMsg(ResultStatus.CLUSTER_NOT_EXIST, MsgConstant.getClusterPhyNotExist(dto.getClusterId()));
        }

        KSGroupDescription description = groupService.getGroupDescriptionFromKafka(clusterPhy, dto.getGroupName());
        if (ConsumerGroupState.DEAD.equals(description.state()) && !dto.isCreateIfNotExist()) {
            return Result.buildFromRSAndMsg(ResultStatus.KAFKA_OPERATE_FAILED, "group不存在, 重置失败");
        }

        if (!ConsumerGroupState.EMPTY.equals(description.state()) && !ConsumerGroupState.DEAD.equals(description.state())) {
            return Result.buildFromRSAndMsg(ResultStatus.KAFKA_OPERATE_FAILED, String.format("group处于%s, 重置失败(仅Empty | Dead 情况可重置)", GroupStateEnum.getByRawState(description.state()).getState()));
        }

        // 获取offset
        Result<Map<TopicPartition, Long>> offsetMapResult = this.getPartitionOffset(dto);
        if (offsetMapResult.failed()) {
            return Result.buildFromIgnoreData(offsetMapResult);
        }

        // 重置offset
        return groupService.resetGroupOffsets(dto.getClusterId(), dto.getGroupName(), offsetMapResult.getData(), operator);
    }

    @Override
    public Result<Void> deleteGroupOffsets(GroupOffsetDeleteDTO dto, String operator) throws Exception {
        ClusterPhy clusterPhy = clusterPhyService.getClusterByCluster(dto.getClusterPhyId());
        if (clusterPhy == null) {
            return Result.buildFromRSAndMsg(ResultStatus.CLUSTER_NOT_EXIST, MsgConstant.getClusterPhyNotExist(dto.getClusterPhyId()));
        }


        // 按照group纬度进行删除
        if (ValidateUtils.isBlank(dto.getGroupName())) {
            return Result.buildFromRSAndMsg(ResultStatus.PARAM_ILLEGAL, "groupName不允许为空");
        }
        if (DeleteGroupTypeEnum.GROUP.getCode().equals(dto.getDeleteType())) {
            return opGroupService.deleteGroupOffset(
                    new DeleteGroupParam(dto.getClusterPhyId(), dto.getGroupName(), DeleteGroupTypeEnum.GROUP),
                    operator
            );
        }


        // 按照topic纬度进行删除
        if (ValidateUtils.isBlank(dto.getTopicName())) {
            return Result.buildFromRSAndMsg(ResultStatus.PARAM_ILLEGAL, "topicName不允许为空");
        }
        if (DeleteGroupTypeEnum.GROUP_TOPIC.getCode().equals(dto.getDeleteType())) {
            return opGroupService.deleteGroupTopicOffset(
                    new DeleteGroupTopicParam(dto.getClusterPhyId(), dto.getGroupName(), DeleteGroupTypeEnum.GROUP, dto.getTopicName()),
                    operator
            );
        }


        // 按照partition纬度进行删除
        if (ValidateUtils.isNullOrLessThanZero(dto.getPartitionId())) {
            return Result.buildFromRSAndMsg(ResultStatus.PARAM_ILLEGAL, "partitionId不允许为空或小于0");
        }
        if (DeleteGroupTypeEnum.GROUP_TOPIC_PARTITION.getCode().equals(dto.getDeleteType())) {
            return opGroupService.deleteGroupTopicPartitionOffset(
                    new DeleteGroupTopicPartitionParam(dto.getClusterPhyId(), dto.getGroupName(), DeleteGroupTypeEnum.GROUP, dto.getTopicName(), dto.getPartitionId()),
                    operator
            );
        }

        return Result.buildFromRSAndMsg(ResultStatus.PARAM_ILLEGAL, "deleteType类型错误");
    }

    @Override
    public List<GroupTopicOverviewVO> getGroupTopicOverviewVOList(Long clusterPhyId, List<GroupMemberPO> groupMemberPOList) {
        // 获取指标
        Result<List<GroupMetrics>> metricsListResult = groupMetricService.listLatestMetricsAggByGroupTopicFromES(
                clusterPhyId,
                groupMemberPOList.stream().map(elem -> new GroupTopic(elem.getGroupName(), elem.getTopicName())).collect(Collectors.toList()),
                Arrays.asList(GroupMetricVersionItems.GROUP_METRIC_LAG),
                AggTypeEnum.MAX
        );
        if (metricsListResult.failed()) {
            // 如果查询失败，则输出错误信息，但是依旧进行已有数据的返回
            LOGGER.error("method=completeMetricData||clusterPhyId={}||result={}||errMsg=search es failed", clusterPhyId, metricsListResult);
        }
        return this.convert2GroupTopicOverviewVOList(groupMemberPOList, metricsListResult.getData());
    }

    @Override
    public List<GroupTopicOverviewVO> getGroupTopicOverviewVOList(Long clusterPhyId, List<GroupMemberPO> poList, Integer timeoutUnitMs) {
        Set<String> requestedGroupSet = new HashSet<>();

        // 获取指标
        Map<String, Map<String, Float>> groupTopicLagMap = new ConcurrentHashMap<>();
        poList.forEach(elem -> {
            if (requestedGroupSet.contains(elem.getGroupName())) {
                // 该Group已经处理过
                return;
            }

            requestedGroupSet.add(elem.getGroupName());
            ApiCallThreadPoolService.runnableTask(
                    String.format("clusterPhyId=%d||groupName=%s||msg=getGroupTopicLag", clusterPhyId, elem.getGroupName()),
                    timeoutUnitMs,
                    () -> {
                        Result<List<GroupMetrics>> listResult = groupMetricService.collectGroupMetricsFromKafka(clusterPhyId, elem.getGroupName(), GroupMetricVersionItems.GROUP_METRIC_LAG);
                        if (listResult == null || !listResult.hasData()) {
                            return;
                        }

                        Map<String, Float> lagMetricMap = new HashMap<>();
                        listResult.getData().forEach(item -> {
                            Float newLag = item.getMetric(GroupMetricVersionItems.GROUP_METRIC_LAG);
                            if (newLag == null) {
                                return;
                            }

                            Float oldLag = lagMetricMap.getOrDefault(item.getTopic(), newLag);
                            lagMetricMap.put(item.getTopic(), Math.max(oldLag, newLag));
                        });

                        groupTopicLagMap.put(elem.getGroupName(), lagMetricMap);
                    }
            );
        });

        ApiCallThreadPoolService.waitResult();

        return this.convert2GroupTopicOverviewVOList(poList, groupTopicLagMap);
    }


    /**************************************************** private method ****************************************************/


    private Result<Void> checkFieldLegal(GroupOffsetResetDTO dto) {
        if (dto == null) {
            return Result.buildFromRSAndMsg(ResultStatus.PARAM_ILLEGAL, "参数为空");
        }

        Topic topic = topicService.getTopic(dto.getClusterId(), dto.getTopicName());
        if (topic == null) {
            return Result.buildFromRSAndMsg(ResultStatus.NOT_EXIST, MsgConstant.getTopicNotExist(dto.getClusterId(), dto.getTopicName()));
        }

        if (OffsetTypeEnum.PRECISE_OFFSET.getResetType() == dto.getResetType()
            && ValidateUtils.isEmptyList(dto.getOffsetList())) {
            return Result.buildFromRSAndMsg(ResultStatus.PARAM_ILLEGAL, "参数错误，指定offset重置需传offset信息");
        }

        if (OffsetTypeEnum.PRECISE_TIMESTAMP.getResetType() == dto.getResetType()
                && ValidateUtils.isNull(dto.getTimestamp())) {
            return Result.buildFromRSAndMsg(ResultStatus.PARAM_ILLEGAL, "参数错误，指定时间重置需传时间信息");
        }

        return Result.buildSuc();
    }

    private Result<Map<TopicPartition, Long>> getPartitionOffset(GroupOffsetResetDTO dto) {
        if (OffsetTypeEnum.PRECISE_OFFSET.getResetType() == dto.getResetType()) {
            return Result.buildSuc(dto.getOffsetList().stream().collect(Collectors.toMap(
                    elem -> new TopicPartition(dto.getTopicName(), elem.getPartitionId()),
                    PartitionOffsetDTO::getOffset,
                    (key1 , key2) -> key2
            )));
        }

        KSOffsetSpec offsetSpec = null;
        if (OffsetTypeEnum.PRECISE_TIMESTAMP.getResetType() == dto.getResetType()) {
            offsetSpec = KSOffsetSpec.forTimestamp(dto.getTimestamp());
        } else if (OffsetTypeEnum.EARLIEST.getResetType() == dto.getResetType()) {
            offsetSpec = KSOffsetSpec.earliest();
        } else {
            offsetSpec = KSOffsetSpec.latest();
        }

        return partitionService.getPartitionOffsetFromKafka(dto.getClusterId(), dto.getTopicName(), offsetSpec);
    }

    private List<GroupTopicOverviewVO> convert2GroupTopicOverviewVOList(List<GroupMemberPO> poList, List<GroupMetrics> metricsList) {
        if (metricsList == null) {
            metricsList = new ArrayList<>();
        }

        // <GroupName, <TopicName, lag>>
        Map<String, Map<String, Float>> metricsMap = new HashMap<>();
        metricsList.stream().forEach(elem -> {
            Float metricValue = elem.getMetrics().get(GroupMetricVersionItems.GROUP_METRIC_LAG);
            if (metricValue == null) {
                return;
            }

            metricsMap.putIfAbsent(elem.getGroup(), new HashMap<>());
            metricsMap.get(elem.getGroup()).put(elem.getTopic(), metricValue);
        });

        return this.convert2GroupTopicOverviewVOList(poList, metricsMap);
    }

    private List<GroupTopicOverviewVO> convert2GroupTopicOverviewVOList(List<GroupMemberPO> poList, Map<String, Map<String, Float>> metricsMap) {
        List<GroupTopicOverviewVO> voList = new ArrayList<>();
        for (GroupMemberPO po: poList) {
            GroupTopicOverviewVO vo = ConvertUtil.obj2Obj(po, GroupTopicOverviewVO.class);
            if (vo == null) {
                continue;
            }

            Float metricValue = metricsMap.getOrDefault(po.getGroupName(), new HashMap<>()).get(po.getTopicName());
            if (metricValue != null) {
                vo.setMaxLag(ConvertUtil.Float2Long(metricValue));
            }

            voList.add(vo);
        }

        return voList;
    }

    private PaginationResult<GroupMetrics> pagingGroupTopicPartitionMetrics(Long clusterPhyId,
                                                                            String groupName,
                                                                            String topicName,
                                                                            List<Integer> partitionIdList,
                                                                            List<String> latestMetricNames,
                                                                            PaginationSortDTO dto) {


        // 获取Group指标信息
        Result<List<GroupMetrics>> groupMetricsResult = groupMetricService.collectGroupMetricsFromKafka(clusterPhyId, groupName, latestMetricNames == null ? Arrays.asList() : latestMetricNames);


        // 转换Group指标
        List<GroupMetrics> esGroupMetricsList = groupMetricsResult.hasData() ? groupMetricsResult.getData().stream().filter(elem -> topicName.equals(elem.getTopic())).collect(Collectors.toList()) : new ArrayList<>();
        Map<Integer, GroupMetrics> esMetricsMap = new HashMap<>();
        for (GroupMetrics groupMetrics: esGroupMetricsList) {
            esMetricsMap.put(groupMetrics.getPartitionId(), groupMetrics);
        }

        List<GroupMetrics> allPartitionGroupMetrics = new ArrayList<>();
        for (Integer partitionId: partitionIdList) {
            allPartitionGroupMetrics.add(esMetricsMap.getOrDefault(partitionId, new GroupMetrics(clusterPhyId, groupName, topicName, partitionId)));
        }

        return PaginationUtil.pageBySubData(
                (List<GroupMetrics>)PaginationMetricsUtil.sortMetrics(allPartitionGroupMetrics, dto.getSortField(), "partitionId", dto.getSortType()),
                dto
        );
    }
}
