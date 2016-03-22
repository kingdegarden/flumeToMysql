package com.cloud.flume.sink.mysql;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.PollableSource;
import org.apache.flume.SystemClock;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.ExecSourceConfigurationConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;



	public class ErrorExecSource extends AbstractSource implements EventDrivenSource,
	Configurable {

	  private static final Logger logger = LoggerFactory
	      .getLogger(ErrorExecSource.class);

	  private String shell;//
	  private String command;//exec执行
	  private SourceCounter sourceCounter;//用于JMX查看的计数器
	  private ExecutorService executor;//具体的Exec执行器
	  private Future<?> runnerFuture;//Exec执行的结果Future
	  private long restartThrottle;//重新执行的间隔
	  private boolean restart;//判断是否命令一次执行成功后需要重新执行
	  private boolean logStderr;//是否输出stderr
	  private Integer bufferCount;//缓冲区数量
	  private long batchTimeout;//批量flush的timeout
	  private ExecRunnable runner;//具体命令的执行代码
	  private Charset charset;//编码
	  
	  private static final String regex = "[0-9]-[a-zA-Z]*-[0-9]";
	  private static final Pattern pattern = Pattern.compile(regex);
	  private static boolean isError = false;
	  private static DateFormat format=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	  @Override
	  public void start() {//重写了父类的start()方法
	    logger.info("Exec source starting with command:{}", command);

	    executor = Executors.newSingleThreadExecutor();//单线程的执行器

	    runner = new ExecRunnable(shell, command, getChannelProcessor(), sourceCounter,
	        restart, restartThrottle, logStderr, bufferCount, batchTimeout, charset);

	    // FIXME: Use a callback-like executor / future to signal us upon failure.
	    runnerFuture = executor.submit(runner);

	    /*
	     * NB: This comes at the end rather than the beginning of the method because
	     * it sets our state to running. We want to make sure the executor is alive
	     * and well first.
	     */
	    sourceCounter.start();
	    super.start();//调用父类start()方法更改Source的状态为started

	    logger.debug("Exec source started");
	  }

	  @Override
	  public void stop() {
	    logger.info("Stopping exec source with command:{}", command);
	    if(runner != null) {
	      runner.setRestart(false);
	      runner.kill();
	    }

	    if (runnerFuture != null) {
	      logger.debug("Stopping exec runner");
	      runnerFuture.cancel(true);
	      logger.debug("Exec runner stopped");
	    }
	    executor.shutdown();

	    while (!executor.isTerminated()) {
	      logger.debug("Waiting for exec executor service to stop");
	      try {
	        executor.awaitTermination(500, TimeUnit.MILLISECONDS);
	      } catch (InterruptedException e) {
	        logger.debug("Interrupted while waiting for exec executor service "
	            + "to stop. Just exiting.");
	        Thread.currentThread().interrupt();
	      }
	    }

	    sourceCounter.stop();
	    super.stop();

	    logger.debug("Exec source with command:{} stopped. Metrics:{}", command,
	        sourceCounter);
	  }
	  //获取用户的flume-ng启动是配置文件中配置的参数
	  public void configure(Context context) {
	    command = context.getString("command");//

	    Preconditions.checkState(command != null,
	        "The parameter command must be specified");

	    restartThrottle = context.getLong(ExecSourceConfigurationConstants.CONFIG_RESTART_THROTTLE,
	        ExecSourceConfigurationConstants.DEFAULT_RESTART_THROTTLE);

	    restart = context.getBoolean(ExecSourceConfigurationConstants.CONFIG_RESTART,
	        ExecSourceConfigurationConstants.DEFAULT_RESTART);

	    logStderr = context.getBoolean(ExecSourceConfigurationConstants.CONFIG_LOG_STDERR,
	        ExecSourceConfigurationConstants.DEFAULT_LOG_STDERR);

	    bufferCount = context.getInteger(ExecSourceConfigurationConstants.CONFIG_BATCH_SIZE,
	        ExecSourceConfigurationConstants.DEFAULT_BATCH_SIZE);

	    batchTimeout = context.getLong(ExecSourceConfigurationConstants.CONFIG_BATCH_TIME_OUT,
	        ExecSourceConfigurationConstants.DEFAULT_BATCH_TIME_OUT);

	    charset = Charset.forName(context.getString(ExecSourceConfigurationConstants.CHARSET,
	        ExecSourceConfigurationConstants.DEFAULT_CHARSET));

	    shell = context.getString(ExecSourceConfigurationConstants.CONFIG_SHELL, null);

	    if (sourceCounter == null) {
	      sourceCounter = new SourceCounter(getName());
	    }
	  }
	  //这个类是重点，包含了Flume怎么执行命令
	  private static class ExecRunnable implements Runnable {

	    public ExecRunnable(String shell, String command, ChannelProcessor channelProcessor,
	        SourceCounter sourceCounter, boolean restart, long restartThrottle,
	        boolean logStderr, int bufferCount, long batchTimeout, Charset charset) {
	      this.command = command;
	      this.channelProcessor = channelProcessor;
	      this.sourceCounter = sourceCounter;
	      this.restartThrottle = restartThrottle;
	      this.bufferCount = bufferCount;
	      this.batchTimeout = batchTimeout;
	      this.restart = restart;
	      this.logStderr = logStderr;
	      this.charset = charset;
	      this.shell = shell;
	    }

	    private final String shell;
	    private final String command;
	    private final ChannelProcessor channelProcessor;
	    private final SourceCounter sourceCounter;
	    private volatile boolean restart;
	    private final long restartThrottle;
	    private final int bufferCount;
	    private long batchTimeout;
	    private final boolean logStderr;
	    private final Charset charset;
	    private Process process = null;
	    private SystemClock systemClock = new SystemClock();
	    private Long lastPushToChannel = systemClock.currentTimeMillis();
	    ScheduledExecutorService timedFlushService;
	    ScheduledFuture<?> future;

	    public void run() {
	      do {
	        String exitCode = "unknown";
	        BufferedReader reader = null;
	        String line = null;
	        final List<Event> eventList = new ArrayList<Event>();//从shell/command读取的内容的缓冲队列，定时刷新到channel

	        timedFlushService = Executors.newSingleThreadScheduledExecutor(
	                new ThreadFactoryBuilder().setNameFormat(
	                "timedFlushExecService" +
	                Thread.currentThread().getId() + "-%d").build());//定时Flush从shell/command的输出到channel的executor
	        try {
	          if(shell != null) {
	            String[] commandArgs = formulateShellCommand(shell, command);
	            process = Runtime.getRuntime().exec(commandArgs);//调用Runtime执行shell
	          }  else {
	            String[] commandArgs = command.split("\\s+");
	            process = new ProcessBuilder(commandArgs).start();//调用ProcessBuilder启动进程
	          }
	          reader = new BufferedReader(
	              new InputStreamReader(process.getInputStream(), charset));//获取进程的输出

	          // StderrLogger dies as soon as the input stream is invalid
	          StderrReader stderrReader = new StderrReader(new BufferedReader(
	              new InputStreamReader(process.getErrorStream(), charset)), logStderr);
	          stderrReader.setName("StderrReader-[" + command + "]");
	          stderrReader.setDaemon(true);
	          stderrReader.start();

	          future = timedFlushService.scheduleWithFixedDelay(new Runnable() {
	              public void run() {
	                try {
	                  synchronized (eventList) {
	                    if(!eventList.isEmpty() && timeout()) {
	                      flushEventBatch(eventList);//如果eventList不为空并且timeout，执行一次flush操作
	                    }
	                  }
	                } catch (Exception e) {
	                  logger.error("Exception occured when processing event batch", e);
	                  if(e instanceof InterruptedException) {
	                      Thread.currentThread().interrupt();
	                  }
	                }
	              }
	          },
	          batchTimeout, batchTimeout, TimeUnit.MILLISECONDS);

	          while ((line = reader.readLine()) != null) {
	        	  //-------------------------------进行过滤操作---------------------------
	        	  if(line.length()>11){
	        		if(pattern.matcher(line.substring(0, 11)).find()){
	        			isError = false;
	        			continue;
	        		}  
	        	  }
	        	  if(line.length()>5){
	        		  if(line.substring(0, 5).equals("ERROR")){
	        			  line = format.format(new Date())+"~"+line;
	        			  isError = true;
	        		  }
	        	  }
	        	  //---------------------------------过滤结束-----------------------------
	        	 if(isError){
	        		 synchronized (eventList) {
	        			 sourceCounter.incrementEventReceivedCount();//
	        			 eventList.add(EventBuilder.withBody(line.getBytes(charset)));//读取到一行内容就对应一个Event
	        			 if(eventList.size() >= bufferCount || timeout()) {//如果缓冲size大于等于最大的缓冲量或者timeout了，直接触发flush
	        				 flushEventBatch(eventList);
	        			 }
	        		 }
	        	 } 
	          }

	          synchronized (eventList) {
	              if(!eventList.isEmpty()) {
	                flushEventBatch(eventList);//最后一次性把没有flush的内容flush
	              }
	          }
	        } catch (Exception e) {
	          logger.error("Failed while running command: " + command, e);
	          if(e instanceof InterruptedException) {
	            Thread.currentThread().interrupt();
	          }
	        } finally {
	          if (reader != null) {
	            try {
	              reader.close();
	            } catch (IOException ex) {
	              logger.error("Failed to close reader for exec source", ex);
	            }
	          }
	          exitCode = String.valueOf(kill());
	        }
	        if(restart) {
	          logger.info("Restarting in {}ms, exit code {}", restartThrottle,
	              exitCode);
	          try {
	            Thread.sleep(restartThrottle);
	          } catch (InterruptedException e) {
	            Thread.currentThread().interrupt();
	          }
	        } else {
	          logger.info("Command [" + command + "] exited with " + exitCode);
	        }
	      } while(restart);
	    }
	    //具体的flush操作
	    private void flushEventBatch(List<Event> eventList){
	      channelProcessor.processEventBatch(eventList);//调用cp来处理
	      sourceCounter.addToEventAcceptedCount(eventList.size());
	      eventList.clear();
	      lastPushToChannel = systemClock.currentTimeMillis();
	    }
	    //timeout 规则，当前时间距离上次flush时间毫秒数是否大于我们配置的batchTimeout
	    private boolean timeout(){
	      return (systemClock.currentTimeMillis() - lastPushToChannel) >= batchTimeout;
	    }

	    private static String[] formulateShellCommand(String shell, String command) {
	      String[] shellArgs = shell.split("\\s+");
	      String[] result = new String[shellArgs.length + 1];
	      System.arraycopy(shellArgs, 0, result, 0, shellArgs.length);
	      result[shellArgs.length] = command;
	      return result;
	    }

	    public int kill() {
	      if(process != null) {
	        synchronized (process) {
	          process.destroy();//杀死进程

	          try {
	            int exitValue = process.waitFor();

	            // Stop the Thread that flushes periodically
	            if (future != null) {
	                future.cancel(true);//停止future
	            }

	            if (timedFlushService != null) {
	              timedFlushService.shutdown();
	              while (!timedFlushService.isTerminated()) {
	                try {
	                  timedFlushService.awaitTermination(500, TimeUnit.MILLISECONDS);//停止定时flush线程
	                } catch (InterruptedException e) {
	                  logger.debug("Interrupted while waiting for exec executor service "
	                    + "to stop. Just exiting.");
	                  Thread.currentThread().interrupt();
	                }
	              }
	            }
	            return exitValue;
	          } catch (InterruptedException ex) {
	            Thread.currentThread().interrupt();
	          }
	        }
	        return Integer.MIN_VALUE;
	      }
	      return Integer.MIN_VALUE / 2;
	    }
	    public void setRestart(boolean restart) {
	      this.restart = restart;
	    }
	  }
	  private static class StderrReader extends Thread {
	    private BufferedReader input;
	    private boolean logStderr;

	    protected StderrReader(BufferedReader input, boolean logStderr) {
	      this.input = input;
	      this.logStderr = logStderr;
	    }

	    @Override
	    public void run() {
	      try {
	        int i = 0;
	        String line = null;
	        while((line = input.readLine()) != null) {
	          if(logStderr) {
	            // There is no need to read 'line' with a charset
	            // as we do not to propagate it.
	            // It is in UTF-16 and would be printed in UTF-8 format.
	            logger.info("StderrLogger[{}] = '{}'", ++i, line);//调用logger输出process的stderr
	          }
	        }
	      } catch (IOException e) {
	        logger.info("StderrLogger exiting", e);
	      } finally {
	        try {
	          if(input != null) {
	            input.close();
	          }
	        } catch (IOException ex) {
	          logger.error("Failed to close stderr reader for exec source", ex);
	        }
	      }
	    }
	  }

}
