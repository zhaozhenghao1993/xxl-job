package com.xxl.job.core.thread;

import com.xxl.job.core.biz.model.HandleCallbackParam;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.biz.model.TriggerParam;
import com.xxl.job.core.executor.XxlJobExecutor;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.log.XxlJobFileAppender;
import com.xxl.job.core.log.XxlJobLogger;
import com.xxl.job.core.util.ShardingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;


/**
 * handler thread
 * @author xuxueli 2016-1-16 19:52:47
 */
public class JobThread extends Thread{
	private static Logger logger = LoggerFactory.getLogger(JobThread.class);

	private int jobId;
	private IJobHandler handler;
	private LinkedBlockingQueue<TriggerParam> triggerQueue;
	private Set<Long> triggerLogIdSet;		// avoid repeat trigger for the same TRIGGER_LOG_ID

	private volatile boolean toStop = false;
	private String stopReason;

    private boolean running = false;    // if running job
	private int idleTimes = 0;			// idel times


	public JobThread(int jobId, IJobHandler handler) {
		this.jobId = jobId;
		this.handler = handler;
		this.triggerQueue = new LinkedBlockingQueue<TriggerParam>();
		this.triggerLogIdSet = Collections.synchronizedSet(new HashSet<Long>());
	}
	public IJobHandler getHandler() {
		return handler;
	}

    /**
	 * 加入一个新任务到列队
     * new trigger to queue
     *
     * @param triggerParam
     * @return
     */
	public ReturnT<String> pushTriggerQueue(TriggerParam triggerParam) {
		// avoid repeat
		// 看这个set里面是否有正准备在触发的任务，存的是任务日志id,避免对同一个TRIGGER_LOG_ID重复触发
		if (triggerLogIdSet.contains(triggerParam.getLogId())) {
			logger.info(">>>>>>>>>>> repeate trigger job, logId:{}", triggerParam.getLogId());
			return new ReturnT<String>(ReturnT.FAIL_CODE, "repeate trigger job, logId:" + triggerParam.getLogId());
		}

		// 缓存记录日志id  列队记录触发任务信息
		triggerLogIdSet.add(triggerParam.getLogId());
		triggerQueue.add(triggerParam);
        return ReturnT.SUCCESS;
	}

    /**
	 * 干掉这个正在执行任务的线程
     * kill job thread
     *
     * @param stopReason
     */
	public void toStop(String stopReason) {
		/**
		 * Thread.interrupt只支持终止线程的阻塞状态(wait、join、sleep)，
		 * 在阻塞出抛出InterruptedException异常,但是并不会终止运行的线程本身；
		 * 所以需要注意，此处彻底销毁本线程，需要通过共享变量方式；
		 */
		this.toStop = true;
		this.stopReason = stopReason;
	}

    /**
	 * 检查当前是否有任务正在执行
     * is running job
     * @return
     */
    public boolean isRunningOrHasQueue() {
        return running || triggerQueue.size()>0;
    }

    @Override
	public void run() {

    	// init
		// 先执行初始化
    	try {
			handler.init();
		} catch (Throwable e) {
    		logger.error(e.getMessage(), e);
		}

		// execute
		// 执行任务
		while(!toStop){
    		// 重置该线程运行状态
			running = false;
			// 该线程启动后，记录次数，如果试了30次还是没有在队列中接收到triggerParam，就删除该线程
			idleTimes++;

            TriggerParam triggerParam = null;
            ReturnT<String> executeResult = null;
            try {
				// to check toStop signal, we need cycle, so wo cannot use queue.take(), instand of poll(timeout)
				// poll 每次在这里等三秒若列队为空，就返回空
				triggerParam = triggerQueue.poll(3L, TimeUnit.SECONDS);
				// 如果此时列队中存在需要执行的任务
				if (triggerParam!=null) {
					// 记录当前线程 正在执行调度任务
					running = true;
					idleTimes = 0;
					// 删除set中存过的日志id,表示当前这个任务正在被消费
					triggerLogIdSet.remove(triggerParam.getLogId());

					// log filename, like "logPath/yyyy-MM-dd/9999.log"
					// 记录日志
					String logFileName = XxlJobFileAppender.makeLogFileName(new Date(triggerParam.getLogDateTim()), triggerParam.getLogId());
					XxlJobFileAppender.contextHolder.set(logFileName);
					ShardingUtil.setShardingVo(new ShardingUtil.ShardingVO(triggerParam.getBroadcastIndex(), triggerParam.getBroadcastTotal()));

					// execute
					XxlJobLogger.log("<br>----------- xxl-job job execute start -----------<br>----------- Param:" + triggerParam.getExecutorParams());

					// 如果调度中心传来的参数，被执行的任务存在超时时间
					if (triggerParam.getExecutorTimeout() > 0) {
						// limit timeout 限制超时
						Thread futureThread = null;
						try {
							final TriggerParam triggerParamTmp = triggerParam;
							// 可以看出RunnableFuture继承了Runnable接口和Future接口，而FutureTask实现了RunnableFuture接口。
							// 所以它既可以作为Runnable被线程执行，又可以作为Future得到Callable的返回值。
							FutureTask<ReturnT<String>> futureTask = new FutureTask<ReturnT<String>>(new Callable<ReturnT<String>>() {
								@Override
								public ReturnT<String> call() throws Exception {
									return handler.execute(triggerParamTmp.getExecutorParams());
								}
							});
							futureThread = new Thread(futureTask);
							futureThread.start();

							// 所以这里 FutureTask 的 get方法为阻塞方法，定义超时时间，超过超时时间还没有获取到结果，则说明超时，抛出异常
							executeResult = futureTask.get(triggerParam.getExecutorTimeout(), TimeUnit.SECONDS);
						} catch (TimeoutException e) {

							XxlJobLogger.log("<br>----------- xxl-job job execute timeout");
							XxlJobLogger.log(e);

							executeResult = new ReturnT<String>(IJobHandler.FAIL_TIMEOUT.getCode(), "job execute timeout ");
						} finally {
							// 最终设置中断标记
							futureThread.interrupt();
						}
					} else {
						// just execute
						// 不存在超时时间就直接执行
						executeResult = handler.execute(triggerParam.getExecutorParams());
					}

					if (executeResult == null) {
						// 如果最后调用handler.execute 没有返回结果，则说明任务执行失败
						executeResult = IJobHandler.FAIL;
					} else {
						// 如果任务结果Msg太长就截取
						executeResult.setMsg(
								(executeResult!=null&&executeResult.getMsg()!=null&&executeResult.getMsg().length()>50000)
										?executeResult.getMsg().substring(0, 50000).concat("...")
										:executeResult.getMsg());
						executeResult.setContent(null);	// limit obj size
					}
					XxlJobLogger.log("<br>----------- xxl-job job execute end(finish) -----------<br>----------- ReturnT:" + executeResult);

				} else {
					// 该线程启动后，记录次数，如果试了30次还是没有在队列中接收到triggerParam，就删除该线程
					if (idleTimes > 30) {
						XxlJobExecutor.removeJobThread(jobId, "executor idel times over limit.");
					}
				}
			} catch (Throwable e) {
				if (toStop) {
					XxlJobLogger.log("<br>----------- JobThread toStop, stopReason:" + stopReason);
				}

				StringWriter stringWriter = new StringWriter();
				e.printStackTrace(new PrintWriter(stringWriter));
				String errorMsg = stringWriter.toString();
				executeResult = new ReturnT<String>(ReturnT.FAIL_CODE, errorMsg);

				XxlJobLogger.log("<br>----------- JobThread Exception:" + errorMsg + "<br>----------- xxl-job job execute end(error) -----------");
			} finally {
            	// 存在调度参数，则说明此次是一次正常的调度执行，将会回调发送结果
                if(triggerParam != null) {
                	// 执行结束后
                    // callback handler info
                    if (!toStop) {
                    	// 正常执行完毕，将任务日志id,任务执行完成时间，任务执行结果，加入到回调 调度中心结果方法的 列队中，
						// 等待TriggerCallbackThread 中的 triggerCallbackThread 线程回调 调度中心
                        // commonm
                        TriggerCallbackThread.pushCallBack(new HandleCallbackParam(triggerParam.getLogId(), triggerParam.getLogDateTim(), executeResult));
                    } else {
                    	// 这里是被非正常强制关闭的任务信息，同样加入回调列队
                        // is killed
                        ReturnT<String> stopResult = new ReturnT<String>(ReturnT.FAIL_CODE, stopReason + " [job running，killed]");
                        TriggerCallbackThread.pushCallBack(new HandleCallbackParam(triggerParam.getLogId(), triggerParam.getLogDateTim(), stopResult));
                    }
                }
            }
        }

        // 到达这个地方，说明任务被强制停止了

		// callback trigger request in queue
		// 此时列队里还有调度请求，循环取出
		while(triggerQueue !=null && triggerQueue.size()>0){
			// 如果此时这个线程对象还添加了别的调度请求，就取出，
			TriggerParam triggerParam = triggerQueue.poll();
			if (triggerParam!=null) {
				// is killed
				// 并都回调告诉 调度中心 这些任务没有执行，任务此次调度列队已经被干掉了
				ReturnT<String> stopResult = new ReturnT<String>(ReturnT.FAIL_CODE, stopReason + " [job not executed, in the job queue, killed.]");
				TriggerCallbackThread.pushCallBack(new HandleCallbackParam(triggerParam.getLogId(), triggerParam.getLogDateTim(), stopResult));
			}
		}

		// destroy
		// 最后调用destroy
		try {
			handler.destroy();
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
		}

		logger.info(">>>>>>>>>>> xxl-job JobThread stoped, hashCode:{}", Thread.currentThread());
	}
}
