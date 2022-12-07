package com.xiaojukeji.know.streaming.km.task.connect.metrics;

import com.didiglobal.logi.job.common.TaskResult;
import com.didiglobal.logi.log.ILog;
import com.didiglobal.logi.log.LogFactory;
import com.xiaojukeji.know.streaming.km.common.bean.entity.connect.ConnectCluster;
import com.xiaojukeji.know.streaming.km.task.connect.AbstractConnectClusterDispatchTask;
import com.xiaojukeji.know.streaming.km.task.service.TaskThreadPoolService;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Metrics相关任务
 */
public abstract class AbstractAsyncMetricsDispatchTask extends AbstractConnectClusterDispatchTask {
    private static final ILog log = LogFactory.getLog(AbstractAsyncMetricsDispatchTask.class);

    public abstract TaskResult processClusterTask(ConnectCluster connectCluster, long triggerTimeUnitMs) throws Exception;

    @Autowired
    private TaskThreadPoolService taskThreadPoolService;

    @Override
    protected TaskResult processSubTask(ConnectCluster connectCluster, long triggerTimeUnitMs) throws Exception {
        return this.asyncProcessSubTask(connectCluster, triggerTimeUnitMs);
    }

    public TaskResult asyncProcessSubTask(ConnectCluster connectCluster, long triggerTimeUnitMs) {
        taskThreadPoolService.submitMetricsTask(
                String.format("taskName=%s||clusterPhyId=%d", this.taskName, connectCluster.getId()),
                this.timeoutUnitSec.intValue() * 1000,
                () -> {
                    try {
                        TaskResult tr = this.processClusterTask(connectCluster, triggerTimeUnitMs);
                        if (TaskResult.SUCCESS_CODE != tr.getCode()) {
                            log.error("class=AbstractAsyncMetricsDispatchTask||taskName={}||connectClusterId={}||taskResult={}||msg=failed", this.taskName, connectCluster.getId(), tr);
                        } else {
                            log.debug("class=AbstractAsyncMetricsDispatchTask||taskName={}||connectClusterId={}||msg=success", this.taskName, connectCluster.getId());
                        }
                    } catch (Exception e) {
                        log.error("class=AbstractAsyncMetricsDispatchTask||taskName={}||connectClusterId={}||errMsg=exception", this.taskName, connectCluster.getId(), e);
                    }
                }
        );

        return TaskResult.SUCCESS;
    }
}
