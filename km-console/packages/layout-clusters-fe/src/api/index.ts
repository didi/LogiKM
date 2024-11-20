/* eslint-disable @typescript-eslint/explicit-module-boundary-types */
const ksPrefix = '/ks-km/api/v3';
const securityPrefix = '/logi-security/api/v1';

function getApi(path: string) {
  return `${ksPrefix}${path}`;
}

// 指标类型对应的 type 值
export enum MetricType {
  Topic = 100,
  Cluster = 101,
  Group = 102,
  Broker = 103,
  Partition = 104,
  Replication = 105,
  Zookeeper = 110,
  Connect = 120,
  Connectors = 121,
  Controls = 901,
  MM2 = 122,
}

const api = {
  // 登录 & 登出
  login: `${securityPrefix}/account/login`,
  logout: `${securityPrefix}/account/logout`,

  // 全局信息
  getVersionInfo: () => getApi('/self/version'),
  getUserInfo: (userId: number) => `${securityPrefix}/user/${userId}`,
  getPermissionTree: `${securityPrefix}/permission/tree`,
  getKafkaVersionItems: () => getApi('/kafka-versions-items'),
  getSupportKafkaVersions: (clusterPhyId: string, type: MetricType) =>
    getApi(`/clusters/${clusterPhyId}/types/${type}/support-kafka-versions`),

  // 生产、消费客户端测试
  postClientConsumer: getApi(`/clients/consumer`),
  postClientProducer: getApi(`/clients/producer`),

  // 集群均衡
  getBalanceList: (clusterPhyId: number) => getApi(`/clusters/${clusterPhyId}/balance-overview`),
  getBrokersMetaList: (clusterPhyId: number) => getApi(`/clusters/${clusterPhyId}/brokers-metadata`),
  getTopicMetaList: (clusterPhyId: number) => getApi(`/clusters/${clusterPhyId}/topics-metadata`),
  balanceStrategy: (clusterPhyId: number) => getApi(`/clusters/${clusterPhyId}/balance-strategy`),
  balancePreview: (clusterPhyId: number) => getApi(`/clusters/${clusterPhyId}/balance-preview`),
  getBalanceHistory: (clusterPhyId: number) => getApi(`/clusters/${clusterPhyId}/balance-history`),
  getBalanceForm: (clusterPhyId: number) => getApi(`/clusters/${clusterPhyId}/balance-config`),
  getBalancePlan: (clusterPhyId: number, jobId: number) => getApi(`/clusters/${clusterPhyId}/balance-plan/${jobId}`),
  getPlatformConfig: (clusterPhyId: number, groupName: string) =>
    getApi(`/platform-configs/clusters/${clusterPhyId}/groups/${groupName}/configs`),
  putPlatformConfig: () => getApi(`/platform-configs`),
  getCartInfo: (clusterPhyId: number) => getApi(`/clusters/${clusterPhyId}/balance-state`),
  // 获取topic元信息
  getTopicsMetaData: (topicName: string, clusterPhyId: number) => getApi(`/clusters/${clusterPhyId}/topics/${topicName}/metadata`),
  getTopicsMetrics: (clusterPhyId: number) => getApi(`/clusters/${clusterPhyId}/topic-metrics`),
  getConsumerGroup: (topicName: string, clusterPhyId: number) => getApi(`/clusters/${clusterPhyId}/topics/${topicName}/groups-basic`),
  getTopicMetaData: (clusterPhyId: number) => getApi(`/clusters/${clusterPhyId}/topics-metadata`),
  getTopicBrokersList: (clusterPhyId: string, topicName: number) => getApi(`/clusters/${clusterPhyId}/topics/${topicName}/brokers`), // 获取 topic brokers 信息
  getPartitionMetricInfo: (clusterPhyId: string, topicName: string, brokerId: number, partitionId: number) =>
    getApi(`/clusters/${clusterPhyId}/brokers/${brokerId}/topics/${topicName}/partitions/${partitionId}/latest-metrics`), // 获取分区详情数据

  // dashbord 接口
  phyClustersDashbord: getApi(`/physical-clusters/dashboard`),
  supportKafkaVersion: getApi(`/support-kafka-versions`),
  phyClusterState: getApi(`/physical-clusters/state`),
  phyClusterHealthState: getApi(`/physical-clusters/health-state`),

  getOperatingStateList: (clusterPhyId: number) => getApi(`/clusters/${clusterPhyId}/groups-overview`),
  getGroupTopicList: (clusterPhyId: number, groupName: string) => getApi(`/clusters/${clusterPhyId}/groups/${groupName}/topics-overview`),

  // 物理集群接口
  phyCluster: getApi(`/physical-clusters`),
  getPhyClusterBasic: (clusterPhyId: number) => getApi(`/physical-clusters/${clusterPhyId}/basic`),
  getPhyClusterMetrics: (clusterPhyId: number) => getApi(`/physical-clusters/${clusterPhyId}/latest-metrics`),
  getClusterBasicExit: (clusterPhyName: string) => getApi(`/physical-clusters/${clusterPhyName}/basic-combine-exist`),
  getClustersVersion: getApi('/physical-clusters/exist-version'),

  kafkaValidator: getApi(`/utils/kafka-validator`),

  // @see https://api-kylin-xg02.intra.xiaojukeji.com/ks-km/swagger-ui.html#/KS-KafkaHealth-%E7%9B%B8%E5%85%B3%E6%8E%A5%E5%8F%A3(REST)/getHealthCheckConfigUsingGET
  getClusterHealthyConfigs: (clusterPhyId: number) => getApi(`/clusters/${clusterPhyId}/health-configs`),
  putPlatformConfigs: getApi(`/platform-configs`),

  getClusterChangeLog: (clusterPhyId: number) => getApi(`/clusters/${clusterPhyId}/change-records`),

  // group详情实时信息
  getTopicGroupMetric: (params: { clusterId: number; topicName: string; groupName: string }) =>
    getApi(`/clusters/${params.clusterId}/topics/${params.topicName}/groups/${params.groupName}/metric`),
  // group详情历史信息
  getConsumersMetadata: (clusterPhyId: number, groupName: string, topicName: string) =>
    getApi(`/clusters/${clusterPhyId}/groups/${groupName}/topics/${topicName}/metadata-combine-exist`),
  getTopicGroupMetricHistory: (clusterPhyId: number) => getApi(`/clusters/${clusterPhyId}/group-metrics`),
  getTopicGroupPartitionsHistory: (clusterPhyId: number, groupName: string) =>
    getApi(`/clusters/${clusterPhyId}/groups/${groupName}/partitions`),
  resetGroupOffset: () => getApi('/group-offsets'),
  getGroupOverview: (clusterPhyId: number) => getApi(`/clusters/${clusterPhyId}/groups-overview`),
  deleteGroupOffset: () => getApi('/group-offsets'),
  // topics列表
  getTopicsList: (clusterPhyId: number) => getApi(`/clusters/${clusterPhyId}/topics-overview`),
  getReassignmentList: () => getApi(`/reassignment/topics-overview`),
  getTaskPlanData: () => getApi(`/reassignment/replicas-change-plan`),
  // 创建topic
  addTopic: () => getApi(`/topics`),
  deleteTopic: () => getApi(`/topics`),
  expandPartitions: () => getApi(`/topics/expand-partitions`),
  getDefaultTopicConfig: (clusterPhyId: number) => getApi(`/clusters/${clusterPhyId}/config-topics/default`),
  getTopicState: (clusterPhyId: number, topicName: string) => getApi(`/clusters/${clusterPhyId}/topics/${topicName}/state`),
  getTopicMetadata: (clusterPhyId: number, topicName: string) =>
    getApi(`/clusters/${clusterPhyId}/topics/${topicName}/metadata-combine-exist`),
  deleteTopicData: () => getApi(`/topics/truncate-topic`),

  // 最新的指标值
  getMetricPointsLatest: (clusterPhyId: number) => getApi(`/physical-clusters/${clusterPhyId}/latest-metrics`),
  getTopicMetricPointsLatest: (clusterPhyId: number, topicName: string) =>
    getApi(`/clusters/${clusterPhyId}/topics/${topicName}/latest-metrics`),
  // 健康检查指标
  getMetricPoints: (clusterPhyId: number) => getApi(`/physical-clusters/${clusterPhyId}/metric-points`),
  // 单个Topic的健康检查指标
  getTopicMetricPoints: (clusterPhyId: number, topicName: string) => getApi(`/clusters/${clusterPhyId}/topics/${topicName}/metric-points`),
  // Broker列表接口
  getBrokersList: (clusterPhyId: number) => getApi(`/clusters/${clusterPhyId}/brokers-overview`),
  // 删除失效Broker
  clearInactiveBrokers: (clusterPhyId: number) => getApi(`/clusters/${clusterPhyId}/brokers-clear`),
  // Broker列表页健康检查指标
  getBrokerMetricPoints: (clusterPhyId: number) => getApi(`/physical-clusters/${clusterPhyId}/latest-metrics`),
  // Controller列表接口 /api/v3/clusters/{clusterPhyId}/controller-history「controller-change-log」
  getChangeLogList: (clusterPhyId: number) => getApi(`/clusters/${clusterPhyId}/controller-history`),
  getBrokersState: (clusterPhyId: string) => getApi(`/clusters/${clusterPhyId}/brokers-state`), // Broker 基础信息

  // Controller列表接口
  // getChangeLogList: (clusterPhyId: number) => getApi(`/clusters/${clusterPhyId}/controller-change-log`),

  // GroupList 列表接口
  getGroupACLBindingList: (clusterPhyId: number) => getApi(`/clusters/${clusterPhyId}/group-acl-bindings`),

  /* Topic 详情 ↓↓↓↓↓↓↓↓↓↓*/
  getTopicPartitionsSummary: (clusterPhyId: string, topicName: string) =>
    getApi(`/clusters/${clusterPhyId}/topics/${topicName}/brokers-partitions-summary`),
  getTopicPartitionsDetail: (clusterPhyId: string, topicName: string) => getApi(`/clusters/${clusterPhyId}/topics/${topicName}/partitions`),
  getTopicMessagesList: (topicName: string, clusterPhyId: number) => getApi(`/clusters/${clusterPhyId}/topics/${topicName}/records`), // Messages列表
  getTopicGroupList: (topicName: string, clusterPhyId: number) => getApi(`/clusters/${clusterPhyId}/topics/${topicName}/groups-overview`), // Consumers列表
  getTopicMessagesMetadata: (topicName: string, clusterPhyId: number) => getApi(`/clusters//${clusterPhyId}/topics/${topicName}/metadata`), // Messages列表
  getTopicACLsList: (topicName: string, clusterPhyId: number) => getApi(`/clusters/${clusterPhyId}/topics/${topicName}/acl-Bindings`), // ACLs列表
  getTopicConfigs: (topicName: string, clusterPhyId: number) => getApi(`/clusters/${clusterPhyId}/config-topics/${topicName}/configs`), // Configuration列表
  getTopicEditConfig: () => getApi('/config-topics'),
  /* Topic 详情 ↑↑↑↑↑↑↑↑↑↑↑*/

  /* Broker 详情 ↓↓↓↓↓↓↓↓↓↓*/
  getBrokerConfigs: (brokerId: number, clusterPhyId: number) => getApi(`/clusters/${clusterPhyId}/config-brokers/${brokerId}/configs`), // Configuration列表
  getBrokerDataLogs: (brokerId: number, clusterPhyId: number) => getApi(`/clusters/${clusterPhyId}/brokers/${brokerId}/log-dirs`), // ACLs列表
  getBrokerMetadata: (brokerId: number | string, clusterPhyId: number | string) =>
    getApi(`/clusters/${clusterPhyId}/brokers/${brokerId}/metadata-combine-exist`), // Broker元数据
  getBrokerDetailMetricPoints: (brokerId: number | string, clusterPhyId: number | string) =>
    getApi(`/clusters/${clusterPhyId}/brokers/${brokerId}/latest-metrics`),
  getBrokerEditConfig: () => getApi('/config-brokers'),
  /* Broker 详情 ↑↑↑↑↑↑↑↑↑↑↑*/
  // 具体资源健康检查详情
  getResourceHealthDetail: (clusterPhyId: number, dimensionCode: number, resName: string) =>
    getApi(`/clusters/${clusterPhyId}/dimensions/${dimensionCode}/resources/${resName}/health-detail`),
  // 列表健康检查详情
  getResourceListHealthDetail: (clusterPhyId: number) => getApi(`/clusters/${clusterPhyId}/health-detail`),

  // Cluster 单集群详情页
  getClusterDefaultMetricData: () => getApi('/physical-clusters/metrics-multi-value'),
  getClusterMetricDataList: () => getApi('/physical-clusters/metrics'),

  // BrokerDashboard & TopicDashboard 相关
  getDashboardMetadata: (clusterPhyId: string, type: MetricType) =>
    getApi(`/clusters/${clusterPhyId}/${MetricType[type].toLowerCase()}s-metadata`), // 集群节点信息
  getDashboardMetricList: (clusterPhyId: string, type: MetricType) => getApi(`/clusters/${clusterPhyId}/types/${type}/user-metric-config`), // 默认选中的指标项
  getDashboardMetricChartData: (clusterPhyId: string, type: MetricType) =>
    getApi(`/clusters/${clusterPhyId}/${MetricType[type].toLowerCase()}-metrics`), // 图表数据

  // ! Jobs 集群任务相关接口
  getJobsList: (clusterPhyId: string) => getApi(`/clusters/${clusterPhyId}/jobs-overview`),
  getJobsState: (clusterPhyId: string) => getApi(`/clusters/${clusterPhyId}/jobs-state`),
  getJobDetail: (clusterPhyId: string, jobId: any) => getApi(`/clusters/${clusterPhyId}/jobs/${jobId}/detail`),
  getJobsPlanRebalance: (clusterPhyId: string, jobId: any) => getApi(`/clusters/${clusterPhyId}/balance-plan/${jobId}`),
  getJobsScheduleRebalance: (clusterPhyId: string, jobId: any) => getApi(`/clusters/${clusterPhyId}/balance-schedule/${jobId}`),
  getJobsDelete: (clusterPhyId: string, jobId: any) => getApi(`/clusters/${clusterPhyId}/jobs/${jobId}`),
  getJobTraffic: (clusterPhyId: string, jobId: any, flowLimit: any) => {
    return getApi(`/clusters/${clusterPhyId}/jobs/${jobId}/traffic/${flowLimit}`);
  },
  getJobNodeTraffic: (clusterPhyId: string, jobId: any) => {
    return getApi(`/clusters/${clusterPhyId}/jobs/${jobId}/node/traffic`);
  },
  getJobPartitionDetail: (clusterPhyId: string, jobId: any, topicName: any) => {
    return getApi(`/clusters/${clusterPhyId}/jobs/${jobId}/${topicName}/partition-detail`);
  },

  // Security - ACLs
  getACLs: (clusterPhyId: string) => getApi(`/clusters/${clusterPhyId}/acl-bindings`),
  addACL: getApi('/kafka-acls/batch'),
  delACLs: getApi('/kafka-acls'),

  // Security - Users
  getKafkaUsers: (clusterPhyId: string) => getApi(`/clusters/${clusterPhyId}/kafka-users`),
  kafkaUser: getApi('/kafka-users'),
  getKafkaUserToken: (clusterPhyId: string, kafkaUser: string) => getApi(`/clusters/${clusterPhyId}/kafka-users/${kafkaUser}/token`),
  updateKafkaUserToken: getApi('/kafka-users/token'),

  //迁移任务、扩缩副本任务
  createTask: (clusterPhyId: string) => getApi(`/clusters/${clusterPhyId}/jobs`),
  //获取topic原数据信息
  getOneTopicMetaData: (clusterPhyId: string, topicName: string) => getApi(`/clusters/${clusterPhyId}/topics/${topicName}/metadata`),
  //获取迁移任务预览
  getMovePlanTaskData: () => getApi(`/reassignment/replicas-move-plan`),
  //获取任务详情
  getJobsTaskData: (clusterPhyId: string, jobId: string | number) => getApi(`/clusters/${clusterPhyId}/jobs/${jobId}/modify-detail`),
  //编辑任务
  putJobsTaskData: (clusterPhyId: string) => getApi(`/clusters/${clusterPhyId}/jobs`),

  // Zookeeper 接口
  getZookeeperState: (clusterPhyId: string) => getApi(`/clusters/${clusterPhyId}/zookeepers-state`),
  getZookeeperList: (clusterPhyId: number) => getApi(`/clusters/${clusterPhyId}/zookeepers-overview`),
  getZookeeperNodeChildren: (clusterPhyId: number) => getApi(`/clusters/${clusterPhyId}/znode-children`),
  getZookeeperNodeData: (clusterPhyId: number) => getApi(`/clusters/${clusterPhyId}/znode-data`),
  getZookeeperMetricsInfo: (clusterPhyId: number) => getApi(`/clusters/${clusterPhyId}/zookeeper-latest-metrics`),
  getZookeeperMetrics: (clusterPhyId: string) => getApi(`/clusters/${clusterPhyId}/zookeeper-metrics`),

  // Connector 接口
  getConnectState: (clusterPhyId: string) => getApi(`/kafka-clusters/${clusterPhyId}/connect-state`),
  getConnectorsList: (clusterPhyId: number) => getApi(`/clusters/${clusterPhyId}/connectors-overview`),
  // Connector 详情
  getConnectDetailMetricPoints: (connectorName: number | string, connectClusterId: number | string) =>
    getApi(`/kafka-connect/clusters/${connectClusterId}/connectors/${connectorName}/latest-metrics`),
  getConnectDetailTasks: (connectorName: number | string, connectClusterId: number | string) =>
    getApi(`/kafka-connect/clusters/${connectClusterId}/connectors/${connectorName}/tasks`),
  getConnectDetailState: (connectorName: number | string, connectClusterId: number | string) =>
    getApi(`/kafka-connect/clusters/${connectClusterId}/connectors/${connectorName}/state`),
  optionTasks: () => getApi(`/kafka-connect/tasks`),
  // Workers 接口
  getWorkersList: (clusterPhyId: number) => getApi(`/clusters/${clusterPhyId}/workers-overview`),
  // Connector
  getConnectClusters: (clusterPhyId: string) => getApi(`/kafka-clusters/${clusterPhyId}/connect-clusters-basic`),
  getConnectClusterMetrics: (clusterPhyId: string) => getApi(`/kafka-clusters/${clusterPhyId}/connect-cluster-metrics`),
  getConnectors: (clusterPhyId: string) => getApi(`/clusters/${clusterPhyId}/connectors-basic`),
  getConnectorMetrics: (clusterPhyId: string) => getApi(`/clusters/${clusterPhyId}/connectors-metrics`),
  getConnectorPlugins: (connectClusterId: number) => getApi(`/kafka-connect/clusters/${connectClusterId}/connector-plugins`),
  getConnectorPluginConfig: (connectClusterId: number | string, pluginName: string) =>
    getApi(`/kafka-connect/clusters/${connectClusterId}/connector-plugins/${pluginName}/config`),
  getCurPluginConfig: (connectClusterId: number | string, connectorName: string) =>
    getApi(`/kafka-connect/clusters/${connectClusterId}/connectors/${connectorName}/config`),
  isConnectorExist: (connectClusterId: number, connectorName: string) =>
    getApi(`/kafka-connect/clusters/${connectClusterId}/connectors/${connectorName}/basic-combine-exist`),
  validateConnectorConfig: getApi('/kafka-connect/connectors-config/validate'),
  // Connector 操作接口 新增、暂停、重启、删除
  connectorsOperates: getApi('/kafka-connect/connectors'),
  // 修改 Connector 配置
  updateConnectorConfig: getApi('/kafka-connect/connectors-config'),
  // Cluster首页修改Connect集群
  batchConnectClusters: getApi(`/kafka-connect/batch-connect-clusters`),
  // Cluster首页删除Connect集群
  deleteConnectClusters: getApi(`/kafka-connect/connect-clusters`),

  getConnectClusterBasicExit: (clusterPhyId: string, clusterPhyName: string) =>
    getApi(`/kafka-clusters/${clusterPhyId}/connect-clusters/${clusterPhyName}/basic-combine-exist`),

  // MM2 列表
  getMirrorMakerList: (clusterPhyId: number) => getApi(`/clusters/${clusterPhyId}/mirror-makers-overview`),
  // MM2 状态卡片
  getMirrorMakerState: (clusterPhyId: string) => getApi(`/kafka-clusters/${clusterPhyId}/mirror-makers-state`),
  // MM2 指标卡片
  getMirrorMakerMetrics: (clusterPhyId: string) => getApi(`/clusters/${clusterPhyId}/mirror-makers-metrics`),
  // MM2 筛选
  getMirrorMakerMetadata: (clusterPhyId: string) => getApi(`/clusters/${clusterPhyId}/mirror-makers-basic`),
  // MM2 详情列表
  getMM2DetailTasks: (connectorName: number | string, connectClusterId: number | string) =>
    getApi(`/kafka-mm2/clusters/${connectClusterId}/connectors/${connectorName}/tasks`),
  // MM2 详情状态卡片
  getMM2DetailState: (connectorName: number | string, connectClusterId: number | string) =>
    getApi(`/kafka-mm2/clusters/${connectClusterId}/connectors/${connectorName}/state`),
  // MM2 操作接口 新增、暂停、重启、删除
  mirrorMakerOperates: getApi('/kafka-mm2/mirror-makers'),
  // MM2 操作接口 新增、编辑校验
  validateMM2Config: getApi('/kafka-mm2/mirror-makers-config/validate'),
  // 修改 Connector 配置
  updateMM2Config: getApi('/kafka-mm2/mirror-makers-config'),
  // MM2 详情
  getMirrorMakerMetricPoints: (mirrorMakerName: number | string, connectClusterId: number | string) =>
    getApi(`/kafka-mm2/clusters/${connectClusterId}/connectors/${mirrorMakerName}/latest-metrics`),
  getSourceKafkaClusterBasic: getApi(`/physical-clusters/basic`),
  getGroupBasic: (clusterPhyId: string) => getApi(`/clusters/${clusterPhyId}/groups-basic`),
  // Topic复制
  getMirrorClusterList: () => getApi(`/ha-mirror/physical-clusters/basic`),
  handleTopicMirror: () => getApi(`/ha-mirror/topics`),
  getTopicMirrorList: (clusterPhyId: number, topicName: string) =>
    getApi(`/ha-mirror/clusters/${clusterPhyId}/topics/${topicName}/mirror-info`),
  getMirrorMakerConfig: (connectClusterId: number | string, connectorName: string) =>
    getApi(`/kafka-mm2/clusters/${connectClusterId}/connectors/${connectorName}/config`),
};

export default api;
