package com.xiaojukeji.know.streaming.km.rebalance.algorithm.optimizer.goals;

import com.xiaojukeji.know.streaming.km.rebalance.algorithm.model.Broker;
import com.xiaojukeji.know.streaming.km.rebalance.algorithm.model.ClusterModel;
import com.xiaojukeji.know.streaming.km.rebalance.algorithm.model.LogDir;
import com.xiaojukeji.know.streaming.km.rebalance.algorithm.model.Replica;
import com.xiaojukeji.know.streaming.km.rebalance.algorithm.optimizer.*;

import java.util.*;
import java.util.stream.Collectors;

public abstract class AbstractLogDirGoal implements Goal {

    /**
     * 均衡算法逻辑处理
     */
    protected abstract void rebalanceForBroker(Broker broker, ClusterModel clusterModel, Set<Goal> optimizedGoals, OptimizationOptions optimizationOptions);

    /**
     * 集群列表中的所有Broker循环执行均衡算法
     */
    @Override
    public void optimize(ClusterModel clusterModel, Set<Goal> optimizedGoals, OptimizationOptions optimizationOptions) {
        initGoalState(clusterModel, optimizationOptions);
        SortedSet<Broker> brokenBrokers = clusterModel.brokers().stream()
                .filter(b -> optimizationOptions.balanceBrokers().isEmpty()
                        || optimizationOptions.balanceBrokers().contains(b.id()))
                .collect(Collectors.toCollection(TreeSet::new));

        // SortedSet<Broker> brokenBrokers = clusterModel.brokers();

        for (Broker broker : brokenBrokers) {
            rebalanceForBroker(broker, clusterModel, optimizedGoals, optimizationOptions);
        }
    }

    protected abstract void initGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions);

    /**
     * 根据已经计算完的均衡副本、候选目标LogDir、执行类型来
     * 执行不同的集群模型数据更改操作
     */
    protected LogDir maybeApplyBalancingAction(ClusterModel clusterModel,
                                               Replica replica,
                                               Collection<LogDir> candidateLogDirs,
                                               ActionType action,
                                               Set<Goal> optimizedGoals,
                                               OptimizationOptions optimizationOptions) {
        //如果该topicPartiton的其他副本已经参与过迁移，则跳过，避免出现tp-replica1:[broker1/data1 -> broker2/data3], tp-replica2:[broker4/data2 -> broker1/data2]的情况（同一个tp的不同副本先从broker1迁走，随后另一个副本又迁回broker1），触发kafka bug:https://issues.apache.org/jira/browse/KAFKA-9087
        if(action == ActionType.REPLICA_MOVEMENT && clusterModel.balanceActionHistory().containsKey(replica.topicPartition())) {
            return null;
        }

        List<LogDir> eligibleLogDirs = eligibleLogDirs(replica, candidateLogDirs, action, optimizationOptions);
        for (LogDir logDir : eligibleLogDirs) {

            BalancingAction proposal = new BalancingAction(replica.topicPartition(), replica.broker().id(), replica.logDir(), logDir.broker().id(), logDir.name(), action);
            //均衡的副本如果存在当前的Broker上则尝试下一个logDir
            if (!legitMove(replica, logDir.broker(), action)) {
                continue;
            }
            //均衡条件不满足则尝试下一个logDir
            if (!selfSatisfied(clusterModel, proposal)) {
                continue;
            }
            //判断当前均衡操作是否与其他目标冲突,如果冲突则禁止均衡操作
            ActionAcceptance acceptance = AnalyzerUtils.isProposalAcceptableForOptimizedGoals(optimizedGoals, proposal, clusterModel);
            if (acceptance == ActionAcceptance.ACCEPT) {
                if (action == ActionType.LEADERSHIP_MOVEMENT) {
                    clusterModel.relocateLeadership(name(), action.toString(), replica.topicPartition(), replica.broker().id(), logDir.broker().id());
                } else if (action == ActionType.REPLICA_MOVEMENT) {
                    clusterModel.relocateReplica(name(), action.toString(), replica.topicPartition(), replica.broker().id(), replica.logDir(), logDir.broker().id(), logDir.name());
                }
                return logDir;
            }
        }
        return null;
    }

    /**
     * 副本操作合法性判断:
     * 1.副本迁移，目的broker不包含移动副本
     * 2.Leader切换，目的broker需要包含切换副本
     */
    private static boolean legitMove(Replica replica,
                                     Broker destinationBroker, ActionType actionType) {
        switch (actionType) {
            case REPLICA_MOVEMENT:
                return destinationBroker.replica(replica.topicPartition()) == null;
            case LEADERSHIP_MOVEMENT:
                return replica.isLeader() && destinationBroker.replica(replica.topicPartition()) != null;
            default:
                return false;
        }
    }

    protected abstract boolean selfSatisfied(ClusterModel clusterModel, BalancingAction action);

    /**
     * 候选Broker列表筛选过滤
     */
    public static List<LogDir> eligibleLogDirs(Replica replica,
                                               Collection<LogDir> candidates,
                                               ActionType action,
                                               OptimizationOptions optimizationOptions) {
        List<LogDir> eligibleBrokers = new ArrayList<>(candidates);
        filterOutBrokersExcludedForLeadership(eligibleBrokers, optimizationOptions, replica, action);
        filterOutBrokersExcludedForReplicaMove(eligibleBrokers, optimizationOptions, action);
        return eligibleBrokers;
    }

    /**
     * Leader切换，从候选的Broker列表中排除掉excludedBroker
     */
    public static void filterOutBrokersExcludedForLeadership(List<LogDir> eligibleLogDirs,
                                                             OptimizationOptions optimizationOptions,
                                                             Replica replica,
                                                             ActionType action) {
        Set<Integer> excludedBrokers = optimizationOptions.offlineBrokers();
        if (!excludedBrokers.isEmpty() && (action == ActionType.LEADERSHIP_MOVEMENT || replica.isLeader())) {
            eligibleLogDirs.removeIf(logDir -> excludedBrokers.contains(logDir.broker().id()));
        }
    }

    /**
     * 副本迁移，从候选的Broker列表中排除掉excludedBroker
     */
    public static void filterOutBrokersExcludedForReplicaMove(List<LogDir> eligibleLogDirs,
                                                              OptimizationOptions optimizationOptions,
                                                              ActionType action) {
        Set<Integer> excludedBrokers = optimizationOptions.offlineBrokers();
        if (!excludedBrokers.isEmpty() && action == ActionType.REPLICA_MOVEMENT) {
            eligibleLogDirs.removeIf(logDir -> excludedBrokers.contains(logDir.broker().id()));
        }
    }
}
