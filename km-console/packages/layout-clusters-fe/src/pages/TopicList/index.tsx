/* eslint-disable react/display-name */
import React, { useState, useEffect } from 'react';
import { useHistory, useParams } from 'react-router-dom';
import {
  AppContainer,
  Input,
  ProTable,
  Select,
  Switch,
  Tooltip,
  Utils,
  Dropdown,
  Menu,
  Button,
  Divider,
  Tag,
  Popconfirm,
  notification,
} from 'knowdesign';
import { IconFont } from '@knowdesign/icons';
import Create from './Create';
import './index.less';
import Api from '@src/api/index';
import ExpandPartition from './ExpandPartition';
import TopicHealthCheck from '@src/components/CardBar/TopicHealthCheck';
import TopicDetail from '../TopicDetail';
import Delete from './Delete';
import { ClustersPermissionMap } from '../CommonConfig';
import DBreadcrumb from 'knowdesign/es/extend/d-breadcrumb';
import ReplicaChange from '@src/components/TopicJob/ReplicaChange';
import SmallChart from '@src/components/SmallChart';
import ReplicaMove from '@src/components/TopicJob/ReplicaMove';
import TopicMirror from '@src/components/TopicJob/TopicMirror';
import { formatAssignSize } from '../Jobs/config';
import { DownOutlined } from '@ant-design/icons';
import { tableHeaderPrefix } from '@src/constants/common';
import { HealthStateMap } from './config';
import { ControlStatusMap } from '../CommonRoute';

const { Option } = Select;

const AutoPage = (props: any) => {
  const routeParams = useParams<{ clusterId: string }>();
  const history = useHistory();
  const [global] = AppContainer.useGlobalValue();
  const [selectedRowKeys, setSelectedRowKeys] = useState<any[]>([]);
  const [selectedRows, setSelectedRows] = useState<any[]>([]);
  const [topicList, setTopicList] = useState([]);
  const [pageIndex, setPageIndex] = useState(1);
  const [pageTotal, setPageTotal] = useState(1);
  const [pageSize, setPageSize] = useState(10);
  const [showInternalTopics, setShowInternalTopics] = useState(false);
  const [searchKeywords, setSearchKeywords] = useState('');
  const [searchKeywordsInput, setSearchKeywordsInput] = useState('');
  const [topicListLoading, setTopicListLoading] = useState(false);
  const [type, setType] = useState<string>('');
  const [changeVisible, setChangeVisible] = useState(false);
  const [moveVisible, setMoveVisible] = useState(false);
  const [mirrorVisible, setMirrorVisible] = useState(false);
  const [selectValue, setSelectValue] = useState('批量操作');

  const [sortObj, setSortObj] = useState<{
    sortField: string;
    sortType: 'desc' | 'asc' | '';
  }>({ sortField: 'createTime', sortType: 'desc' });
  const getRecent1DayTimeStamp = () => [Date.now() - 24 * 60 * 60 * 1000, Date.now()];
  const getTopicsList = () => {
    const [startStamp, endStamp] = getRecent1DayTimeStamp();
    // const [startStamp, endStamp] = [1650445216000, 1650448576988]
    const params: any = {
      metricLines: {
        aggType: 'avg',
        endTime: endStamp,
        metricsNames: ['HealthScore', 'BytesIn', 'BytesOut', 'LogSize'],
        startTime: startStamp,
        topNu: 0,
      },
      latestMetricNames: ['HealthScore', 'BytesIn', 'BytesOut', 'LogSize'],
      pageNo: pageIndex,
      pageSize: pageSize,
      searchKeywords,
      showInternalTopics,
      sortType: sortObj.sortType || 'desc',
      sortField: sortObj.sortField || 'createTime',
    };
    // if (sortObj.sortField && sortObj.sortType) {
    //   params.sortField = sortObj.sortField;
    //   params.sortType = sortObj.sortType || 'desc';
    // }
    setTopicListLoading(true);
    Utils.post(Api.getTopicsList(Number(routeParams.clusterId)), params)
      .then((data: any) => {
        setTopicListLoading(false);
        setTopicList(data?.bizData || []);
        // setPageIndex(data.pagination.pageNo)
        setPageTotal(data.pagination.total);
        // setPageSize(data.pagination.pageSize)
      })
      .catch((e) => {
        setTopicListLoading(false);
      });
  };
  const deleteTopicData = (record: any) => {
    console.log(record, 'record');
    const params = {
      clusterId: Number(routeParams.clusterId),
      topicName: record.topicName,
    };
    Utils.post(Api.deleteTopicData(), params).then((data: any) => {
      if (data === null) {
        notification.success({
          message: '清除数据成功',
        });
        getTopicsList();
      }
    });
  };
  useEffect(() => {
    getTopicsList();
  }, [sortObj, showInternalTopics, searchKeywords, pageIndex, pageSize]);

  const calcCurValue = (record: any, metricName: string) => {
    // const item = (record.metricPoints || []).find((item: any) => item.metricName === metricName);
    // return item?.value || '';
    const orgVal = record?.latestMetrics?.metrics?.[metricName];
    if (orgVal !== undefined) {
      if (metricName === 'HealthState') {
        return HealthStateMap[orgVal] || '-';
      } else if (metricName === 'LogSize') {
        return Number(Utils.formatAssignSize(orgVal, 'MB')).toLocaleString();
      } else {
        return Number(Utils.formatAssignSize(orgVal, 'KB')).toLocaleString();
        // return Utils.formatAssignSize(orgVal, 'KB');
      }
    }
    return '-';
    // return orgVal !== undefined ? (metricName !== 'HealthScore' ? formatAssignSize(orgVal, 'KB') : orgVal) : '-';
  };
  const renderLine = (record: any, metricName: string) => {
    const points = record.metricLines.find((item: any) => item.metricName === metricName)?.metricPoints || [];
    return (
      <div className="metric-data-wrap">
        <SmallChart
          width={'100%'}
          height={30}
          chartData={{
            name: record.metricName,
            data: points.map((item: any) => ({ time: item.timeStamp, value: item.value })),
          }}
        />
        <span className="cur-val">{calcCurValue(record, metricName)}</span>
      </div>
    );
  };

  const columns = () => {
    const baseColumns: any = [
      {
        title: 'TopicName',
        dataIndex: 'topicName',
        key: 'topicName',
        fixed: 'left',
        width: 140,
        className: 'clean-padding-left',
        lineClampOne: true,
        // eslint-disable-next-line react/display-name
        render: (t: string, record: any) => {
          return (
            <>
              <Tooltip title={t}>
                <a
                  onClick={() => {
                    window.location.hash = `topicName=${t}`;
                  }}
                >
                  {t}
                </a>
              </Tooltip>
              {record.inMirror && (
                <div>
                  <Tag
                    style={{
                      color: '#5664FF',
                      padding: '2px 5px',
                      background: '#eff1fd',
                      marginLeft: '-4px',
                      transform: 'scale(0.83,0.83)',
                    }}
                  >
                    复制中...
                  </Tag>
                </div>
              )}
            </>
          );
        },
      },
      {
        title: 'Partitions',
        dataIndex: 'partitionNum',
        key: 'partitionNum',
        width: 100,
      },
      {
        title: 'Replications',
        dataIndex: 'replicaNum',
        key: 'replicaNum',
        width: 100,
      },
      {
        title: '健康状态',
        dataIndex: 'HealthState',
        key: 'HealthState',
        sorter: true,
        // 设计图上量出来的是144，但做的时候发现写144 header部分的sort箭头不出来，所以临时调大些
        width: 100,
        render: (value: any, record: any) => {
          return calcCurValue(record, 'HealthState');
        },
      },
      // {
      //   title: '创建时间',
      //   dataIndex: 'createTime',
      //   key: 'createTime',
      //   width: 240,
      //   render: (v: string) => moment(new Date(v)).format('YYYY-MM-DD HH:mm:ss')
      // },
      {
        title: 'Bytes In(KB/s)',
        dataIndex: 'BytesIn',
        key: 'BytesIn',
        sorter: true,
        width: 170,
        render: (value: any, record: any) => renderLine(record, 'BytesIn'),
      },
      {
        title: 'Bytes Out(KB/s)',
        dataIndex: 'BytesOut',
        key: 'BytesOut',
        sorter: true,
        width: 170,
        render: (value: any, record: any) => renderLine(record, 'BytesOut'),
      },
      {
        title: 'MessageSize(MB)',
        dataIndex: 'LogSize',
        key: 'LogSize',
        sorter: true,
        width: 170,
        render: (value: any, record: any) => renderLine(record, 'LogSize'),
      },
      {
        title: '保存时间(h)',
        dataIndex: 'retentionTimeUnitMs',
        key: 'retentionTimeUnitMs',
        width: 120,
        render: (v: any) => {
          return (v / 3600000).toFixed(2);
        },
      },
      {
        title: '描述',
        dataIndex: 'description',
        key: 'description',
        lineClampTwo: true,
        width: 150,
        needTooltip: true,
      },
    ];

    if (
      global.hasPermission &&
      (global.hasPermission(ClustersPermissionMap.TOPIC_EXPOND) || global.hasPermission(ClustersPermissionMap.TOPIC_DEL))
    ) {
      baseColumns.push({
        title: '操作',
        dataIndex: 'desc',
        key: 'desc',
        fixed: 'right',
        width: 200,
        render: (value: any, record: any) => {
          return (
            <div className="operation-list">
              {global.hasPermission(ClustersPermissionMap.TOPIC_EXPOND) ? (
                <ExpandPartition record={record} onConfirm={getTopicsList}></ExpandPartition>
              ) : (
                <></>
              )}
              {global.hasPermission(ClustersPermissionMap.TOPIC_DEL) ? <Delete record={record} onConfirm={getTopicsList}></Delete> : <></>}
              {global.hasPermission(ClustersPermissionMap.TOPIC_DEL) ? ( // TODO：替换为清除数据的权限
                <Popconfirm
                  placement="topRight"
                  title={`是否要清空当前Topic的数据？`}
                  onConfirm={() => deleteTopicData(record)}
                  okText="是"
                  cancelText="否"
                >
                  <Button type="link">清除数据</Button>
                </Popconfirm>
              ) : (
                <></>
              )}
            </div>
          );
        },
      });
    }

    return baseColumns;
  };
  const onSelectChange = (selectedRowKeys: any, selectedRows: any) => {
    setSelectedRowKeys(selectedRowKeys);
    setSelectedRows(selectedRows);
  };
  const rowSelection = {
    selectedRowKeys,
    onChange: onSelectChange,
  };
  const onclose = () => {
    setChangeVisible(false);
    setMoveVisible(false);
    setMirrorVisible(false);
    setSelectValue('批量操作');
  };

  const menu = (
    <Menu>
      {global.hasPermission(ClustersPermissionMap.TOPIC_CHANGE_REPLICA) && (
        <Menu.Item>
          <a onClick={() => setChangeVisible(true)}>扩缩副本</a>
        </Menu.Item>
      )}
      {global.hasPermission(ClustersPermissionMap.TOPIC_MOVE_REPLICA) && (
        <Menu.Item>
          <a onClick={() => setMoveVisible(true)}>迁移副本</a>
        </Menu.Item>
      )}
      {global.hasPermission(ClustersPermissionMap.TOPIC_REPLICATOR) && (
        <Menu.Item>
          <a onClick={() => setMirrorVisible(true)}>Topic复制</a>
        </Menu.Item>
      )}
    </Menu>
  );

  return (
    <>
      <div className="breadcrumb" style={{ marginBottom: '10px' }}>
        <DBreadcrumb
          breadcrumbs={[
            { label: '多集群管理', aHref: '/' },
            { label: global?.clusterInfo?.name, aHref: `/cluster/${global?.clusterInfo?.id}` },
            { label: 'Topic', aHref: `/cluster/${routeParams?.clusterId}/topic` },
            { label: 'Topics', aHref: `` },
          ]}
        />
      </div>
      <div style={{ margin: '12px 0' }}>
        <TopicHealthCheck></TopicHealthCheck>
      </div>
      <div className="custom-table-content">
        <div className={`${tableHeaderPrefix}`}>
          <div className={`${tableHeaderPrefix}-left`}>
            {/* 批量扩缩副本 */}
            <ReplicaChange drawerVisible={changeVisible} jobId={''} topics={selectedRowKeys} onClose={onclose}></ReplicaChange>
            {/* 批量迁移 */}
            <ReplicaMove drawerVisible={moveVisible} jobId={''} topics={selectedRowKeys} onClose={onclose}></ReplicaMove>
            {/* Topic复制 */}
            <TopicMirror drawerVisible={mirrorVisible} genData={getTopicsList} onClose={onclose}></TopicMirror>

            <div className={`${tableHeaderPrefix}-left-refresh`} onClick={() => getTopicsList()}>
              <IconFont className={`${tableHeaderPrefix}-left-refresh-icon`} type="icon-shuaxin1" />
            </div>
            <Divider type="vertical" className={`${tableHeaderPrefix}-divider`} />
            <div className="internal-switch">
              <Switch
                size="small"
                checked={showInternalTopics}
                onChange={(checked) => {
                  setShowInternalTopics(checked);
                }}
              />
              <span>展示系统Topic</span>
            </div>
          </div>
          <div className={`${tableHeaderPrefix}-right`}>
            <Input
              className="search-input"
              suffix={
                <IconFont
                  type="icon-fangdajing"
                  onClick={(_) => {
                    setSearchKeywords(searchKeywordsInput);
                    setPageIndex(1);
                  }}
                  style={{ fontSize: '16px' }}
                />
              }
              placeholder="请输入TopicName"
              value={searchKeywordsInput}
              onPressEnter={(_) => {
                setSearchKeywords(searchKeywordsInput);
                setPageIndex(1);
              }}
              onChange={(e) => {
                setSearchKeywordsInput(e.target.value);
              }}
            />
            {(global.hasPermission(ClustersPermissionMap.TOPIC_CHANGE_REPLICA) ||
              global.hasPermission(ClustersPermissionMap.TOPIC_MOVE_REPLICA) ||
              global.hasPermission(ClustersPermissionMap.TOPIC_REPLICATOR)) && (
              <Dropdown overlay={menu} trigger={['click']}>
                <Button className="batch-btn" icon={<DownOutlined />} type="primary" ghost>
                  批量变更
                </Button>
              </Dropdown>
            )}
            {global.hasPermission && global.hasPermission(ClustersPermissionMap.TOPIC_ADD) ? (
              <Create onConfirm={getTopicsList}></Create>
            ) : (
              <></>
            )}
          </div>
        </div>
        {/* <Table rowKey={'topicName'} columns={columns} dataSource={topicList} scroll={{ x: 1500 }} /> */}
        <ProTable
          showQueryForm={false}
          tableProps={{
            loading: topicListLoading,
            showHeader: false,
            rowKey: 'topicName',
            columns: columns(),
            dataSource: topicList,
            lineFillColor: true,
            paginationProps:
              pageTotal > 0
                ? {
                    current: pageIndex,
                    total: pageTotal,
                    pageSize: pageSize,
                  }
                : null,
            attrs: {
              className: `topic-list`,
              bordered: false,
              // rowSelection: rowSelection,
              scroll: { x: 'max-content', y: 'calc(100vh - 400px)' },
              sortDirections: ['descend', 'ascend', 'default'],
              onChange: (pagination: any, filters: any, sorter: any) => {
                setSortObj({
                  sortField: sorter.field || '',
                  sortType: sorter.order ? sorter.order.substring(0, sorter.order.indexOf('end')) : '',
                });
                setPageIndex(pagination.current);
                setPageSize(pagination.pageSize);
              },
            },
          }}
        />
      </div>
      {<TopicDetail />}
    </>
  );
};

export default AutoPage;
