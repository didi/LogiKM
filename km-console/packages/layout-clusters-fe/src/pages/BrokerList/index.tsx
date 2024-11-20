import React, { useState, useEffect, memo } from 'react';
import { useParams, useHistory, useLocation } from 'react-router-dom';
import { ProTable, Drawer, Utils, AppContainer, SearchInput } from 'knowdesign';
import { IconFont } from '@knowdesign/icons';
import API from '../../api';
import { getBrokerListColumns, defaultPagination } from './config';
import { tableHeaderPrefix } from '@src/constants/common';
import BrokerDetail from '../BrokerDetail';
import CardBar from '@src/components/CardBar';
import BrokerHealthCheck from '@src/components/CardBar/BrokerHealthCheck';
import DBreadcrumb from 'knowdesign/es/extend/d-breadcrumb';
import './index.less';
const { request } = Utils;

const BrokerList: React.FC = (props: any) => {
  const [global] = AppContainer.useGlobalValue();

  const urlParams = useParams<any>(); // 获取地址栏参数
  const history = useHistory();
  const [loading, setLoading] = useState(false);
  const [data, setData] = useState([]);
  const [searchKeywords, setSearchKeywords] = useState('');
  // const [filteredInfo, setFilteredInfo] = useState(null);
  const [pagination, setPagination] = useState<any>(defaultPagination);
  const [clusterName, setClusterName] = useState<any>(null);
  // 默认排序
  const defaultSorter = {
    sortField: 'brokerId',
    sortType: 'asc',
  };

  // 请求接口获取数据
  const genData = async ({ pageNo, pageSize, filters, sorter }: any) => {
    if (urlParams?.clusterId === undefined) return;
    // filters = filters || filteredInfo;
    setLoading(true);
    const params = {
      searchKeywords: searchKeywords.slice(0, 128),
      pageNo,
      pageSize,
      latestMetricNames: ['PartitionsSkew', 'Leaders', 'LeadersSkew', 'LogSize'],
      sortField: sorter?.field || 'brokerId',
      sortType: sorter?.order ? sorter.order.substring(0, sorter.order.indexOf('end')) : 'asc',
    };

    // API.getBrokersList(urlParams?.clusterId)
    request(API.getBrokersList(urlParams?.clusterId), { method: 'POST', data: params })
      .then((res: any) => {
        setPagination({
          current: res.pagination?.pageNo,
          pageSize: res.pagination?.pageSize,
          total: res.pagination?.total,
        });
        const newData =
          res?.bizData.map((item: any) => {
            return {
              ...item,
              ...item?.latestMetrics?.metrics,
            };
          }) || [];
        setData(newData);
        setLoading(false);
      })
      .catch((err) => {
        setLoading(false);
      });
  };

  // 请求接口获取数据
  const clearInactiveBrokers = async ({ pageNo, pageSize, filters, sorter }: any) => {
    if (urlParams?.clusterId === undefined) return;
    // filters = filters || filteredInfo;
    setLoading(true);
    const params = {
      searchKeywords: searchKeywords.slice(0, 128),
      pageNo,
      pageSize,
      latestMetricNames: ['PartitionsSkew', 'Leaders', 'LeadersSkew', 'LogSize'],
      sortField: sorter?.field || 'brokerId',
      sortType: sorter?.order ? sorter.order.substring(0, sorter.order.indexOf('end')) : 'asc',
    };

    request(API.clearInactiveBrokers(urlParams?.clusterId), { method: 'POST', data: params })
      .then((res: any) => {
        setPagination({
          current: res.pagination?.pageNo,
          pageSize: res.pagination?.pageSize,
          total: res.pagination?.total,
        });
        const newData =
          res?.bizData.map((item: any) => {
            return {
              ...item,
              ...item?.latestMetrics?.metrics,
            };
          }) || [];
        setData(newData);
        setLoading(false);
      })
      .catch((err) => {
        setLoading(false);
      });
  };

  const onTableChange = (pagination: any, filters: any, sorter: any) => {
    // setFilteredInfo(filters);
    genData({ pageNo: pagination.current, pageSize: pagination.pageSize, filters, sorter });
  };

  const getSearchKeywords = (value: string) => {
    setSearchKeywords(value);
  };

  useEffect(() => {
    genData({
      pageNo: 1,
      pageSize: pagination.pageSize,
      // sorter: defaultSorter
    });
  }, [searchKeywords]);

  return (
    <div key="brokerList" className="brokerList">
      <div className="breadcrumb" style={{ marginBottom: '10px' }}>
        <DBreadcrumb
          breadcrumbs={[
            { label: '多集群管理', aHref: '/' },
            { label: global?.clusterInfo?.name, aHref: `/cluster/${global?.clusterInfo?.id}` },
            { label: 'Broker', aHref: `/cluster/${urlParams?.clusterId}/broker` },
            { label: 'Brokers', aHref: `` },
          ]}
        />
      </div>
      <div style={{ margin: '12px 0' }}>
        <BrokerHealthCheck />
      </div>
      <div className="custom-table-content">
        <div className={tableHeaderPrefix}>
          <div className={`${tableHeaderPrefix}-left`}>
            <div
              className={`${tableHeaderPrefix}-left-refresh`}
              onClick={() => genData({ pageNo: pagination.current, pageSize: pagination.pageSize })}
            >
              <IconFont className={`${tableHeaderPrefix}-left-refresh-icon`} type="icon-shuaxin1" />
            </div>
            <div
              className={`${tableHeaderPrefix}-left-clear`}
              onClick={() => clearInactiveBrokers({ pageNo: pagination.current, pageSize: pagination.pageSize })}
            >
              <IconFont className={`${tableHeaderPrefix}-left-clear-icon`} type="icon-Operation" />
            </div>
          </div>
          <div className={`${tableHeaderPrefix}-right`}>
            <SearchInput
              onSearch={setSearchKeywords}
              attrs={{
                placeholder: '请输入Broker Host',
                style: { width: '248px', borderRiadus: '8px' },
                maxLength: 128,
              }}
            />
          </div>
        </div>
        <ProTable
          key="brokerTable"
          showQueryForm={false}
          tableProps={{
            showHeader: false,
            rowKey: 'broker_list',
            loading: loading,
            columns: getBrokerListColumns(),
            dataSource: data,
            paginationProps: { ...pagination },
            attrs: {
              onChange: onTableChange,
              scroll: { x: 'max-content', y: 'calc(100vh - 400px)' },
              bordered: false,
            },
          }}
        />
      </div>
      {<BrokerDetail />}
    </div>
  );
};

export default BrokerList;
