package com.xxl.job.core.biz.impl;

import com.xxl.job.core.biz.ExecutorBiz;
import com.xxl.job.core.biz.model.LogResult;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.biz.model.TriggerParam;
import com.xxl.job.core.enums.ExecutorBlockStrategyEnum;
import com.xxl.job.core.executor.XxlJobExecutor;
import com.xxl.job.core.glue.GlueFactory;
import com.xxl.job.core.glue.GlueTypeEnum;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.impl.GlueJobHandler;
import com.xxl.job.core.handler.impl.ScriptJobHandler;
import com.xxl.job.core.log.XxlJobFileAppender;
import com.xxl.job.core.thread.JobThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * Created by xuxueli on 17/3/1.
 */
public class ExecutorBizImpl implements ExecutorBiz {
    private static Logger logger = LoggerFactory.getLogger(ExecutorBizImpl.class);

    @Override
    public ReturnT<String> beat() {
        return ReturnT.SUCCESS;
    }

    @Override
    public ReturnT<String> idleBeat(int jobId) {

        // isRunningOrHasQueue
        boolean isRunningOrHasQueue = false;
        JobThread jobThread = XxlJobExecutor.loadJobThread(jobId);
        if (jobThread != null && jobThread.isRunningOrHasQueue()) {
            isRunningOrHasQueue = true;
        }

        if (isRunningOrHasQueue) {
            return new ReturnT<String>(ReturnT.FAIL_CODE, "job thread is running or has trigger queue.");
        }
        return ReturnT.SUCCESS;
    }

    @Override
    public ReturnT<String> kill(int jobId) {
        // kill handlerThread, and create new one
        // 从缓存中获取jobId对应 JobThread 执行线程
        JobThread jobThread = XxlJobExecutor.loadJobThread(jobId);
        if (jobThread != null) {
            // 存在就把它删掉 根本不会停止业务任务
            XxlJobExecutor.removeJobThread(jobId, "scheduling center kill job.");
            return ReturnT.SUCCESS;
        }

        return new ReturnT<String>(ReturnT.SUCCESS_CODE, "job thread aleady killed.");
    }

    @Override
    public ReturnT<LogResult> log(long logDateTim, long logId, int fromLineNum) {
        // log filename: logPath/yyyy-MM-dd/9999.log
        String logFileName = XxlJobFileAppender.makeLogFileName(new Date(logDateTim), logId);

        LogResult logResult = XxlJobFileAppender.readLog(logFileName, fromLineNum);
        return new ReturnT<LogResult>(logResult);
    }

    /**
     * 调度中心远程调用，触发一次任务调度
     * @param triggerParam
     * @return
     */
    @Override
    public ReturnT<String> run(TriggerParam triggerParam) {
        // load old：jobHandler + jobThread
        // 从 缓存中获取对应任务id的 JobThread 线程，避免快速多次调度任务时重复创建线程，而且该线程会停留 (3 * 30)s
        JobThread jobThread = XxlJobExecutor.loadJobThread(triggerParam.getJobId());
        // 获取该线程中存在的 IJobHandler 执行器 handler
        IJobHandler jobHandler = jobThread!=null?jobThread.getHandler():null;
        String removeOldReason = null;

        // valid：jobHandler + jobThread
        GlueTypeEnum glueTypeEnum = GlueTypeEnum.match(triggerParam.getGlueType());
        if (GlueTypeEnum.BEAN == glueTypeEnum) {
            // BEAN 类型


            // new jobhandler
            // 根据执行器启动时 XxlJobSpringExecutor 中缓存的 JobHandler
            IJobHandler newJobHandler = XxlJobExecutor.loadJobHandler(triggerParam.getExecutorHandler());

            // valid old jobThread 验证老的 jobThread
            // 如果该 任务执行线程还健在 但是 两个 jobHandler 实例不一样，说明有问题，就置空重新获取
            if (jobThread!=null && jobHandler != newJobHandler) {
                // change handler, need kill old thread
                // 原因就是 同一个 jobId 但是更换了 jobhandler
                removeOldReason = "change jobhandler or glue type, and terminate the old job thread.";

                jobThread = null;
                jobHandler = null;
            }

            // valid handler
            // 如果该 任务执行线程还健在但是 jobHandler为空 或者 任务执行线程为空 或者 上面给置空，就让新从缓存中获取的 jobHandler 替换
            if (jobHandler == null) {
                jobHandler = newJobHandler;
                if (jobHandler == null) {
                    // 如果获取不到就报错 任务触发失败 填写的执行器不存在
                    return new ReturnT<String>(ReturnT.FAIL_CODE, "job handler [" + triggerParam.getExecutorHandler() + "] not found.");
                }
            }

        } else if (GlueTypeEnum.GLUE_GROOVY == glueTypeEnum) {

            // valid old jobThread
            if (jobThread != null &&
                    !(jobThread.getHandler() instanceof GlueJobHandler
                        && ((GlueJobHandler) jobThread.getHandler()).getGlueUpdatetime()==triggerParam.getGlueUpdatetime() )) {
                // change handler or gluesource updated, need kill old thread
                removeOldReason = "change job source or glue type, and terminate the old job thread.";

                jobThread = null;
                jobHandler = null;
            }

            // valid handler
            if (jobHandler == null) {
                try {
                    IJobHandler originJobHandler = GlueFactory.getInstance().loadNewInstance(triggerParam.getGlueSource());
                    jobHandler = new GlueJobHandler(originJobHandler, triggerParam.getGlueUpdatetime());
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                    return new ReturnT<String>(ReturnT.FAIL_CODE, e.getMessage());
                }
            }
        } else if (glueTypeEnum!=null && glueTypeEnum.isScript()) {

            // valid old jobThread
            if (jobThread != null &&
                    !(jobThread.getHandler() instanceof ScriptJobHandler
                            && ((ScriptJobHandler) jobThread.getHandler()).getGlueUpdatetime()==triggerParam.getGlueUpdatetime() )) {
                // change script or gluesource updated, need kill old thread
                removeOldReason = "change job source or glue type, and terminate the old job thread.";

                jobThread = null;
                jobHandler = null;
            }

            // valid handler
            if (jobHandler == null) {
                jobHandler = new ScriptJobHandler(triggerParam.getJobId(), triggerParam.getGlueUpdatetime(), triggerParam.getGlueSource(), GlueTypeEnum.match(triggerParam.getGlueType()));
            }
        } else {
            return new ReturnT<String>(ReturnT.FAIL_CODE, "glueType[" + triggerParam.getGlueType() + "] is not valid.");
        }

        // executor block strategy
        // 阻塞处理策略
        if (jobThread != null) {
            // 如果 该执行线程不为空
            ExecutorBlockStrategyEnum blockStrategy = ExecutorBlockStrategyEnum.match(triggerParam.getExecutorBlockStrategy(), null);
            if (ExecutorBlockStrategyEnum.DISCARD_LATER == blockStrategy) {
                // 丢弃后续调度(从代码来看应该是，如果之前的调度还在执行，则丢弃掉后面触发的太快的任务)
                // discard when running
                // 当前有任务正在执行或者列队中存在准备调度的任务
                if (jobThread.isRunningOrHasQueue()) {
                    return new ReturnT<String>(ReturnT.FAIL_CODE, "block strategy effect："+ExecutorBlockStrategyEnum.DISCARD_LATER.getTitle());
                }
            } else if (ExecutorBlockStrategyEnum.COVER_EARLY == blockStrategy) {
                // 覆盖之前的调度 (制空之前的线程，让重新创建)
                // kill running jobThread
                if (jobThread.isRunningOrHasQueue()) {
                    removeOldReason = "block strategy effect：" + ExecutorBlockStrategyEnum.COVER_EARLY.getTitle();

                    jobThread = null;
                }
            } else {
                // just queue trigger
            }
        }

        // replace thread (new or exists invalid)
        // 替换新的 线程 (创建或替换无效的) 并且开始执行，开始让新的 jobThread 还是while，等待pushTriggerQueue
        if (jobThread == null) {
            jobThread = XxlJobExecutor.registJobThread(triggerParam.getJobId(), jobHandler, removeOldReason);
        }

        // push data to queue
        // 往队列中存入 触发参数，此时任务开始执行，返回触发结果
        ReturnT<String> pushResult = jobThread.pushTriggerQueue(triggerParam);
        return pushResult;
    }

}
