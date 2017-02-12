/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nutch.fetcher;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

// Slf4j Logging imports
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.*;
import org.apache.nutch.protocol.*;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.util.*;

public class NIOFetcher extends NutchTool implements Tool,
    MapRunnable<Text, CrawlDatum, Text, NutchWritable> {

  public static final int PERM_REFRESH_TIME = 5;

  public static final String CONTENT_REDIR = "content";

  public static final String PROTOCOL_REDIR = "protocol";

  public static final Logger LOG = LoggerFactory.getLogger(NIOFetcher.class);

  private OutputCollector<Text, NutchWritable> output;
  private Reporter reporter;

  private String segmentName;
  // / TODO: 活动线程没有意义，选择其他的抓取状态跟踪，现在只作为抓取线程是否结束标志
  private AtomicInteger activeThreads = new AtomicInteger(0);
  private AtomicInteger spinWaiting = new AtomicInteger(0);

  private long start = System.currentTimeMillis(); // start time of fetcher run
  private AtomicLong lastRequestStart = new AtomicLong(start);

  private AtomicLong bytes = new AtomicLong(0); // total bytes fetched
  private AtomicInteger pages = new AtomicInteger(0); // total pages fetched
  private AtomicInteger errors = new AtomicInteger(0); // total pages errored

  private AtomicInteger timeouts = new AtomicInteger(0);
  private AtomicInteger actives = new AtomicInteger(0);

  private boolean storingContent;
  private boolean parsing;
  FetchItemQueues fetchQueues;
  QueueFeeder feeder;

  private static class CrawlState {
    public int state;
    public long lastOpTime;
    public SelectionKey key;

    public final static byte CONNECT = 0;
    public final static byte READ = 1;
  }

  // / TODO: 处理HTTPS协议
  private class PageHandler implements Runnable {
    private BlockingQueue<Page> pagesQueue;

    private final byte[] EMPTY_CONTENT = new byte[0];

    private Configuration conf;

    private URLFilters urlFilters;
    private URLNormalizers urlNormalizers;

    private ScoringFilters scfilters;

    private FetchOutputer fetchOutputer;
    private RedirectInfo redirectInfo = new RedirectInfo();

    // Used by the REST service
    private FetchNode fetchNode;
    private boolean reportToNutchServer;

    public PageHandler(BlockingQueue<Page> pagesQueue, Configuration conf) {
      this.pagesQueue = pagesQueue;
      this.conf = conf;

      urlFilters = new URLFilters(conf);
      urlNormalizers = new URLNormalizers(conf, "");

      this.scfilters = new ScoringFilters(conf);

      fetchOutputer = new FetchOutputer(conf, fetchQueues, reporter,
          segmentName, parsing, output, storingContent, redirectInfo, fetchNode);
    }

    @Override
    public void run() {
      while (true) {
        try {
          Page page = pagesQueue.take();

          if (page.isEndPage())
            break;

          String url = page.getUri();

          // LOG.info("Handling " + url);
          page.process();

          CrawlDatum datum = page.getDatum();
          datum.setFetchTime(System.currentTimeMillis());
          if (page.isFetchFailed()) {
            fetchOutputer.output(new Text(url), datum, null,
                ProtocolStatus.STATUS_RETRY, CrawlDatum.STATUS_FETCH_RETRY);
            continue;
          }

          int code = page.getStatusCode();
          int status = CrawlDatum.STATUS_FETCH_RETRY;
          if (code == 200) {
            status = CrawlDatum.STATUS_FETCH_SUCCESS;
          } else if (code == 410) { // page is gone
            status = CrawlDatum.STATUS_FETCH_GONE;
          } else if (code >= 300 && code < 400) { // handle redirect
            // TODO: 重定向处理
            String newUrl = page.getHeader("Location");
            if (newUrl != null) {
              try {
                newUrl = urlNormalizers.normalize(newUrl, "");
                newUrl = urlFilters.filter(newUrl);

                if (newUrl != null) {
                  CrawlDatum newDatum = new CrawlDatum(
                      CrawlDatum.STATUS_LINKED, datum.getFetchInterval());
                  // fetchOut.append(new Text(newUrl), newDatum);
                }
              } catch (IOException e) {

              }
            }
            switch (code) {
            case 300: // multiple choices, preferred value in Location
              status = CrawlDatum.STATUS_FETCH_REDIR_PERM;
              break;
            case 301: // moved permanently
            case 305: // use proxy (Location is URL of proxy)
              status = CrawlDatum.STATUS_FETCH_REDIR_PERM;
              break;
            case 302: // found (temporarily moved)
            case 303: // see other (redirect after POST)
            case 307: // temporary redirect
              status = CrawlDatum.STATUS_DB_REDIR_TEMP;
              break;
            case 304: // not modified
              status = CrawlDatum.STATUS_FETCH_NOTMODIFIED;
              break;
            default:
              status = CrawlDatum.STATUS_FETCH_REDIR_PERM;
            }
          } else if (code == 400) { // bad request, mark as GONE
            status = CrawlDatum.STATUS_FETCH_GONE;
          } else if (code == 401) { // requires authorization, but no valid auth
                                    // provided.
            status = CrawlDatum.STATUS_FETCH_RETRY;
          } else if (code == 404) {
            status = CrawlDatum.STATUS_FETCH_GONE;
          } else if (code == 410) { // permanently GONE
            status = CrawlDatum.STATUS_FETCH_GONE;
          } else {
            status = CrawlDatum.STATUS_FETCH_RETRY;
          }

          datum.setStatus(status);

          byte[] content = page.getContent();
          Content c = new Content(page.getUri(), page.getUri(),
              (content == null ? EMPTY_CONTENT : content),
              page.getHeader("Content-Type"), page.getHeaders(), conf);

          fetchOutputer.output(new Text(url), datum, c, null, status);

        } catch (Exception e) {
          // XXX: 此处失败没有将数据保存到crawl-fetch
          LOG.warn(e.toString());
          continue;
        }
      }

      LOG.info("退出页面处理线程");
    }

    // private void output(Text key, CrawlDatum datum, Content content,
    // ProtocolStatus pstatus, int status) {
    // output(key, datum, content, pstatus, status, 0);
    // }
    //
    // private void output(Text key, CrawlDatum datum, Content content,
    // ProtocolStatus pstatus, int status, int outlinkDepth) {
    //
    // datum.setStatus(status);
    // datum.setFetchTime(System.currentTimeMillis());
    // if (pstatus != null)
    // datum.getMetaData().put(Nutch.WRITABLE_PROTO_STATUS_KEY, pstatus);
    //
    // ParseResult parseResult = null;
    // if (content != null) {
    // Metadata metadata = content.getMetadata();
    //
    // // store the guessed content type in the crawldatum
    // if (content.getContentType() != null)
    // datum.getMetaData().put(new Text(Metadata.CONTENT_TYPE),
    // new Text(content.getContentType()));
    //
    // // add segment to metadata
    // metadata.set(Nutch.SEGMENT_NAME_KEY, segmentName);
    // // add score to content metadata so that ParseSegment can pick it up.
    // try {
    // scfilters.passScoreBeforeParsing(key, datum, content);
    // } catch (Exception e) {
    // if (LOG.isWarnEnabled()) {
    // LOG.warn("Couldn't pass score, url " + key + " (" + e + ")");
    // }
    // }
    //
    // /*
    // * Store status code in content So we can read this value during parsing
    // * (as a separate job) and decide to parse or not.
    // */
    // content.getMetadata().add(Nutch.FETCH_STATUS_KEY,
    // Integer.toString(status));
    // }
    //
    // try {
    // output.collect(key, new NutchWritable(datum));
    // if (content != null && storingContent)
    // output.collect(key, new NutchWritable(content));
    // } catch (IOException e) {
    // if (LOG.isErrorEnabled()) {
    // LOG.error("fetcher caught:" + e.toString());
    // }
    // }
    // }
  }

  private class FetcherThread extends Thread {
    private Configuration conf;

    // / IO选择器
    private Selector selector;

    private ByteBuffer readBuffer = ByteBuffer.allocate(8192);
    private Map<String, ByteBuffer> writeBuffers = new HashMap<String, ByteBuffer>();
    private Map<String, ByteArrayOutputStream> streams = new HashMap<String, ByteArrayOutputStream>();

    private HttpRequestBuilder httpRequestBuilder = new HttpRequestBuilder();

    // / select操作等待，毫秒，<=0,立即返回
    private int selectTimeout = 2000;

    // / 处理中的异步请求数
    // / XXX: 和actives有何区别？
    private int inProgress = 0;

    // / 最大处理异步请求数
    private int maxInProgress = 200;

    // / 抓取队列中是否还有链接
    private boolean hasUrls = true;

    // TODO: 区分连接超时和读取超时
    // / 异步请求超时
    private int timeout;

    // / 异步请求超时检查间隔，毫秒
    private int timeoutCheckInterval = 3000;

    // / 上次异步请求超时检查时间
    private long lastTimeoutCheck = 0;

    // / 异步请求状态，链接 -> 状态
    private HashMap<String, CrawlState> states = new HashMap<String, CrawlState>();

    // / 待处理的抓取内容
    private BlockingQueue<Page> pagesQueue;

    public FetcherThread(BlockingQueue<Page> pagesQueue, Configuration conf)
        throws IOException {
      this.setDaemon(true); // don't hang JVM on exit
      this.setName("FetcherThread"); // use an informative name

      this.pagesQueue = pagesQueue;

      this.conf = conf;

      selectTimeout = conf.getInt("http.nio.timeout.select", 1000);
      timeout = conf.getInt("http.nio.timeout", 30000);
      maxInProgress = conf.getInt("http.nio.request.max", 200);
      timeoutCheckInterval = conf.getInt("http.nio.timeout.interval", 4000);

      selector = Selector.open();
    }

    public void fetch() {
      while (true) {
        initiateNewConnections();

        // 选择就绪事件
        int nb;
        try {
          if (selectTimeout <= 0)
            nb = selector.selectNow();
          else
            nb = selector.select(selectTimeout);
        } catch (IOException e) {
          LOG.warn("nio select: " + e.toString());
          continue;
        }

        // if (nb == 0)
        // 超时检查
        if (System.currentTimeMillis() - lastTimeoutCheck >= timeoutCheckInterval) {
          lastTimeoutCheck = System.currentTimeMillis();
          checkTimeout();
        }

        if (inProgress == 0 && !hasUrls) // 无处理中的请求和无可抓取的链接
          break;

        // 对所有就绪事件进行依次处理
        Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
        while (iter.hasNext()) {
          SelectionKey key = iter.next();
          iter.remove();

          // if (!key.isValid()) {
          // continue;
          // }

          try {
            if (key.isConnectable()) { // 可以连接
              connect(key);
            } else if (key.isWritable()) { // 可以发送HTTP请求
              write(key);
            } else if (key.isReadable()) { // 可以读取服务器数据
              read(key);
            }
          } catch (Exception e) {
            FetchItem att = (FetchItem) key.attachment();
            String url = att.getUrl().toString();
            // LOG.warn(url + ": " + e);
            logError(new Text(url), e.toString());

            finishChannel(key, CrawlDatum.STATUS_FETCH_RETRY, null);
          }

        }

        // checkTimeout();

      }

      LOG.info("退出抓取循环");
      pagesQueue.add(Page.EndPage);
    }

    /**
     * 检查和中止超时的异步请求
     */
    private void checkTimeout() {
      LOG.info("超时检查...");
      HashMap<String, CrawlState> copyStates = new HashMap<String, CrawlState>();
      for (Map.Entry<String, CrawlState> e : states.entrySet()) {
        copyStates.put(e.getKey(), e.getValue());
      }

      long current = System.currentTimeMillis();

      for (Map.Entry<String, CrawlState> e : copyStates.entrySet()) {
        CrawlState s = e.getValue();
        if (current - s.lastOpTime > timeout) {
          // LOG.warn(e.getKey() + ": time out");
          timeouts.incrementAndGet();
          finishChannel(s.key, CrawlDatum.STATUS_FETCH_RETRY, null);
        }
        // if (s.State == 0 && current - s.lastOp > connectTimeout)
        // {
        // LOG.warn(e.getKey() + ": connect time out");
        // finishChannel(s.key, CrawlDatum.STATUS_FETCH_RETRY, null);
        // errConnect++;
        // }
        // else if (s.State == 2 && current - s.lastOp > readTimeout)
        // {
        // LOG.warn(e.getKey() + ": read time out");
        //
        // finishChannel(s.key, CrawlDatum.STATUS_FETCH_RETRY, null);
        // errRead++;
        // }
      }

      copyStates.clear();
    }

    /**
     * 发送异步连接请求
     */
    private void initiateNewConnections() {
      while (inProgress < maxInProgress && hasUrls) {
        FetchItem fit = fetchQueues.getFetchItem();
        if (fit == null) {
          if (feeder.isAlive() || fetchQueues.getTotalSize() > 0) { // 抓取队列有链接，但礼貌性原因无暂无可抓链接
            LOG.debug(getName() + " spin-waiting ...");
            spinWaiting.incrementAndGet();

            // // XXX: 单抓取线程，需要等待吗？
            // try {
            // Thread.sleep(500);
            // } catch (Exception e) {
            // }
            // // XXX: 重置有意义吗？
            // spinWaiting.decrementAndGet();
            // // XXX: 应该退出连接，继续其他工作
            // break;
            //return;
          } else { // 无可抓链接
            hasUrls = false;
            LOG.info("Thread " + getName() + " has no more work available");
            //return;
          }
          
          return;
        }

        lastRequestStart.set(System.currentTimeMillis());

        // / TODO: 没有IP也可以进行处理
        SocketChannel socketChannel = null;
        SelectionKey key = null;
        try {
          int port = fit.u.getPort();
          port = port > 0 ? port : 80;
          String ipStr = IPUtils.getIPString(fit.datum);

          // XXX: 没有查到IP, CrawlDatum中如何记录这个信息
          // if (ipStr.length() == 0) {
          // noip++;
          // // LOG.warn(url + ": 没有查到IP");
          // Page page = new Page(url.toString(), datum);
          //
          // pagesQueue.add(page);
          // continue;
          // }

          // LOG.info("Connecting " + url.toString() + "...");

          // TODO: 没有进行robots.txt检查
          InetSocketAddress ia = null;
          InetAddress addr = IPUtils.toIP(ipStr);
          ia = new InetSocketAddress(addr, port);
          // if (ia.isUnresolved())
          // continue;

          // LOG.info("Fetching " + fit.url);

          socketChannel = SocketChannel.open();
          socketChannel.configureBlocking(false);
          socketChannel.connect(ia);
          key = socketChannel.register(selector, SelectionKey.OP_CONNECT);

          // Attachment att = new Attachment();
          // att.url = fit.url.toString();
          // att.datum = fit.datum;

          key.attach(fit);
          streams.put(fit.url.toString(), new ByteArrayOutputStream());

          CrawlState s = new CrawlState();
          s.key = key;
          s.state = CrawlState.CONNECT;
          s.lastOpTime = System.currentTimeMillis();
          states.put(fit.url.toString(), s);

          // XXX: 这两个量是否可合并
          inProgress++;
          actives.incrementAndGet();
        } catch (IOException e) {
          // TODO: 异常处理结构放在哪里更合适？
          fetchQueues.finishFetchItem(fit);

//          LOG.warn(fit.u + ": " + e);
          logError(fit.url, e.toString());
          
          if (key != null)
            key.cancel();
          if (socketChannel != null) {
            try {
              socketChannel.close();
            } catch (IOException e1) {
              LOG.info(e.toString());
            }
          }
        }
      }
    }

    /**
     * 完成异步连接
     * 
     * @param key
     *          选择键
     * @throws IOException
     */
    private void connect(SelectionKey key) throws IOException {
      SocketChannel socketChannel = (SocketChannel) key.channel();
      FetchItem att = (FetchItem) key.attachment();
      String url = att.getUrl().toString();

      socketChannel.finishConnect();
      // LOG.info(url + ": connected");

      key.interestOps(SelectionKey.OP_WRITE);

      // CrawlState stat = states.get(url);
      // stat.lastOpTime = System.currentTimeMillis();
      updateState(url);
    }

    /**
     * 更新连接的异步处理状态
     * 
     * @param url
     *          链接
     */
    private void updateState(String url) {
      // TODO: 抓取状态未更新
      CrawlState stat = states.get(url);
      stat.lastOpTime = System.currentTimeMillis();
    }

    /**
     * 发送HTTP请求头到服务器
     * 
     * @param key
     *          选择键
     * @throws IOException
     */
    private void write(SelectionKey key) throws IOException {
      FetchItem att = (FetchItem) key.attachment();
      String url = att.getUrl().toString();
      SocketChannel socketChannel = (SocketChannel) key.channel();

      ByteBuffer writeBuffer = writeBuffers.get(url);
      if (writeBuffer == null) {
        String getRequest = httpRequestBuilder.buildGet(url);
        writeBuffer = ByteBuffer.wrap(getRequest.getBytes());
        writeBuffers.put(url, writeBuffer);

      }

      socketChannel.write(writeBuffer);

      if (!writeBuffer.hasRemaining()) {
        writeBuffers.remove(url);
        key.interestOps(SelectionKey.OP_READ);
      }

      // CrawlState stat = states.get(url);
      // stat.lastOp = System.currentTimeMillis();
      updateState(url);

      // LOG.info(url + ": requested");
    }

    /**
     * 从服务器读取响应
     * 
     * @param key
     *          选择键
     * @throws IOException
     */
    private void read(SelectionKey key) throws IOException {
      FetchItem att = (FetchItem) key.attachment();
      String url = att.getUrl().toString();
      SocketChannel socketChannel = (SocketChannel) key.channel();

      readBuffer.clear();
      int numRead = 0;

      numRead = socketChannel.read(readBuffer);

      // CrawlState stat = states.get(url);
      // stat.lastOp = System.currentTimeMillis();
      // stat.State = 2;
      updateState(url);

      if (numRead > 0) {
        streams.get(url).write(readBuffer.array(), 0, numRead);

        // bytes += numRead;
        // bytesNow += numRead;
      } else if (numRead == -1) {
        ByteArrayOutputStream stream = streams.remove(url);
        finishChannel(key, CrawlDatum.STATUS_FETCH_SUCCESS,
            stream.toByteArray());

        // LOG.info(url + ": finished***");
      }
    }

    /**
     * 完成和关闭通道
     * 
     * @param key
     *          选择键
     * @param status
     *          结果状态
     * @param bytes
     *          服务器响应数据
     */
    private void finishChannel(SelectionKey key, int status, byte[] bytes) {
      FetchItem att = (FetchItem) key.attachment();
      String url = att.getUrl().toString();

      fetchQueues.finishFetchItem(att);

      Page page = null;
      if (bytes == null)
        page = new Page(url, att.datum);
      else
        page = new Page(url, att.datum, bytes);

      pagesQueue.add(page);
      
      if (status == CrawlDatum.STATUS_FETCH_SUCCESS)
        updateStatus(bytes.length);
      
      //
      // LOG.info(url + ": 通道关闭");
      try {
        key.channel().close();

        // inProgress--;
      } catch (IOException e) {
        LOG.warn(url + ": " + e);
      }

      key.cancel();
      states.remove(url);
      streams.remove(url);

      actives.decrementAndGet();
      inProgress--;

      // sockets--;
      //
      // closed++;
    }

    @SuppressWarnings("fallthrough")
    public void run() {
      // TODO: activeThreads没有意义了，只有一个抓取线程，活动连接也许更有意义？
      activeThreads.incrementAndGet(); // count threads

      fetch();

      activeThreads.decrementAndGet(); // count threads

      LOG.info("-finishing fetcher thread, activeRequests=" + actives);
    }

    private void logError(Text url, String message) {
      if (LOG.isInfoEnabled()) {
        LOG.info("fetch of " + url + " failed with: " + message);
      }
      errors.incrementAndGet();
    }
  }

  public NIOFetcher() {
    super(null);
  }

  public NIOFetcher(Configuration conf) {
    super(conf);
  }

  private void updateStatus(int bytesInPage) {
    pages.incrementAndGet();
    bytes.addAndGet(bytesInPage);
  }

  private void reportStatus(int pagesLastSec, int bytesLastSec)
      throws IOException {
    StringBuilder status = new StringBuilder();
    Long elapsed = new Long((System.currentTimeMillis() - start) / 1000);

    float avgPagesSec = (float) pages.get() / elapsed.floatValue();
    long avgBytesSec = (bytes.get() / 125l) / elapsed.longValue();

    status.append(actives).append(" requests (").append(spinWaiting.get())
        .append(" waiting), ");
    status.append(fetchQueues.getQueueCount()).append(" queues, ");
    status.append(fetchQueues.getTotalSize()).append(" URLs queued, ");
    status.append(pages).append(" pages, ").append(errors).append(" errors, ")
        .append(timeouts).append(" timeouts, ");
    status.append(String.format("%.2f", avgPagesSec)).append(" pages/s (");
    status.append(pagesLastSec).append(" last sec), ");
    status.append(avgBytesSec).append(" kbits/s (")
        .append((bytesLastSec / 125)).append(" last sec)");

    LOG.info(status.toString());

    reporter.setStatus(status.toString());
  }

  public void configure(JobConf job) {
    setConf(job);

    this.segmentName = job.get(Nutch.SEGMENT_NAME_KEY);
    this.storingContent = isStoringContent(job);
    this.parsing = isParsing(job);

    // if (job.getBoolean("fetcher.verbose", false)) {
    // LOG.setLevel(Level.FINE);
    // }
  }

  public void close() {
  }

  public static boolean isParsing(Configuration conf) {
    return conf.getBoolean("fetcher.parse", true);
  }

  public static boolean isStoringContent(Configuration conf) {
    return conf.getBoolean("fetcher.store.content", true);
  }

  public void run(RecordReader<Text, CrawlDatum> input,
      OutputCollector<Text, NutchWritable> output, Reporter reporter)
      throws IOException {

    this.output = output;
    this.reporter = reporter;
    this.fetchQueues = new FetchItemQueues(getConf());

    int threadCount = getConf().getInt("fetcher.threads.fetch", 10);

    int timeoutDivisor = getConf().getInt("fetcher.threads.timeout.divisor", 2);
    if (LOG.isInfoEnabled()) {
      LOG.info("NIOFetcher: time-out divisor: " + timeoutDivisor);
    }

    int queueDepthMuliplier = getConf().getInt(
        "fetcher.queue.depth.multiplier", 50);

    feeder = new QueueFeeder(input, fetchQueues, threadCount
        * queueDepthMuliplier);
    // feeder.setPriority((Thread.MAX_PRIORITY + Thread.NORM_PRIORITY) / 2);

    // the value of the time limit is either -1 or the time where it should
    // finish
    long timelimit = getConf().getLong("fetcher.timelimit", -1);
    if (timelimit != -1)
      feeder.setTimeLimit(timelimit);
    feeder.start();

    // 抓取页面
    BlockingQueue<Page> pagesQueue = new LinkedBlockingQueue<Page>();

    // 唯一异步抓取线程
    FetcherThread t = new FetcherThread(pagesQueue, getConf());
    t.start();

    // 抓取内容处理线程
    PageHandler parser = new PageHandler(pagesQueue, getConf());
    Thread thread = new Thread(parser);
    thread.start();

    // select a timeout that avoids a task timeout
    long timeout = getConf().getInt("mapred.task.timeout", 10 * 60 * 1000)
        / timeoutDivisor;

    // Used for threshold check, holds pages and bytes processed in the last
    // second
    int pagesLastSec;
    int bytesLastSec;

    // Set to true whenever the threshold has been exceeded for the first time
    boolean throughputThresholdExceeded = false;
    int throughputThresholdNumRetries = 0;

    int throughputThresholdPages = getConf().getInt(
        "fetcher.throughput.threshold.pages", -1);
    if (LOG.isInfoEnabled()) {
      LOG.info("NIOFetcher: throughput threshold: " + throughputThresholdPages);
    }
    int throughputThresholdMaxRetries = getConf().getInt(
        "fetcher.throughput.threshold.retries", 5);
    if (LOG.isInfoEnabled()) {
      LOG.info("NIOFetcher: throughput threshold retries: "
          + throughputThresholdMaxRetries);
    }
    long throughputThresholdTimeLimit = getConf().getLong(
        "fetcher.throughput.threshold.check.after", -1);

    do {
      // wait for threads to exit
      pagesLastSec = pages.get();
      bytesLastSec = (int) bytes.get();

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }

      pagesLastSec = pages.get() - pagesLastSec;
      bytesLastSec = (int) bytes.get() - bytesLastSec;

      reporter.incrCounter("FetcherStatus", "bytes_downloaded", bytesLastSec);

      reportStatus(pagesLastSec, bytesLastSec);

      LOG.info("-activeRequests=" + actives + ", spinWaiting="
          + spinWaiting.get() + ", fetchQueues.totalSize="
          + fetchQueues.getTotalSize() + ", fetchQueues.getQueueCount="
          + fetchQueues.getQueueCount());

      if (!feeder.isAlive() && fetchQueues.getTotalSize() < 5) {
        fetchQueues.dump();
      }

      // if throughput threshold is enabled
      if (throughputThresholdTimeLimit < System.currentTimeMillis()
          && throughputThresholdPages != -1) {
        // Check if we're dropping below the threshold
        if (pagesLastSec < throughputThresholdPages) {
          throughputThresholdNumRetries++;
          LOG.warn(Integer.toString(throughputThresholdNumRetries)
              + ": dropping below configured threshold of "
              + Integer.toString(throughputThresholdPages)
              + " pages per second");

          // Quit if we dropped below threshold too many times
          if (throughputThresholdNumRetries == throughputThresholdMaxRetries) {
            LOG.warn("Dropped below threshold too many times, killing!");

            // Disable the threshold checker
            throughputThresholdPages = -1;

            // Empty the queues cleanly and get number of items that were
            // dropped
            int hitByThrougputThreshold = fetchQueues.emptyQueues();

            if (hitByThrougputThreshold != 0)
              reporter.incrCounter("FetcherStatus", "hitByThrougputThreshold",
                  hitByThrougputThreshold);
          }
        }
      }

      // / TODO:带宽控制有必要，但实现思路不一样

      // check timelimit
      if (!feeder.isAlive()) {
        int hitByTimeLimit = fetchQueues.checkTimelimit();
        if (hitByTimeLimit != 0)
          reporter.incrCounter("FetcherStatus", "hitByTimeLimit",
              hitByTimeLimit);
      }

      // some requests seem to hang, despite all intentions
      if ((System.currentTimeMillis() - lastRequestStart.get()) > timeout) {
        if (LOG.isWarnEnabled()) {
          // LOG.warn("Aborting with " + activeThreads + " hung threads.");
          LOG.warn("Aborting with " + actives + " hung requests.");
        }
        return;
      }

    } while (activeThreads.get() > 0);
    // LOG.info("-activeThreads=" + activeThreads);
    LOG.info("-activeRequests=" + actives);

  }

  public void fetch(Path segment) throws IOException {

    checkConfiguration();

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    if (LOG.isInfoEnabled()) {
      LOG.info("NIOFetcher: starting at " + sdf.format(start));
      LOG.info("NIOFetcher: segment: " + segment);
    }

    // set the actual time for the timelimit relative
    // to the beginning of the whole job and not of a specific task
    // otherwise it keeps trying again if a task fails
    long timelimit = getConf().getLong("fetcher.timelimit.mins", -1);
    if (timelimit != -1) {
      timelimit = System.currentTimeMillis() + (timelimit * 60 * 1000);
      LOG.info("Fetcher Timelimit set for : " + timelimit);
      getConf().setLong("fetcher.timelimit", timelimit);
    }

    // Set the time limit after which the throughput threshold feature is
    // enabled
    timelimit = getConf().getLong("fetcher.throughput.threshold.check.after",
        10);
    timelimit = System.currentTimeMillis() + (timelimit * 60 * 1000);
    getConf().setLong("fetcher.throughput.threshold.check.after", timelimit);

    int maxOutlinkDepth = getConf().getInt("fetcher.follow.outlinks.depth", -1);
    if (maxOutlinkDepth > 0) {
      LOG.info("NIOFetcher: following outlinks up to depth: "
          + Integer.toString(maxOutlinkDepth));

      int maxOutlinkDepthNumLinks = getConf().getInt(
          "fetcher.follow.outlinks.num.links", 4);
      int outlinksDepthDivisor = getConf().getInt(
          "fetcher.follow.outlinks.depth.divisor", 2);

      int totalOutlinksToFollow = 0;
      for (int i = 0; i < maxOutlinkDepth; i++) {
        totalOutlinksToFollow += (int) Math.floor(outlinksDepthDivisor
            / (i + 1) * maxOutlinkDepthNumLinks);
      }

      LOG.info("NIOFetcher: maximum outlinks to follow: "
          + Integer.toString(totalOutlinksToFollow));
    }

    JobConf job = new NutchJob(getConf());
    job.setJobName("fetch " + segment);

    job.set(Nutch.SEGMENT_NAME_KEY, segment.getName());

    // for politeness, don't permit parallel execution of a single task
    job.setSpeculativeExecution(false);

    FileInputFormat.addInputPath(job, new Path(segment,
        CrawlDatum.GENERATE_DIR_NAME));
    job.setInputFormat(FetcherInputFormat.class);

    job.setMapRunnerClass(NIOFetcher.class);

    FileOutputFormat.setOutputPath(job, segment);
    job.setOutputFormat(FetcherOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NutchWritable.class);

    JobClient.runJob(job);

    long end = System.currentTimeMillis();
    LOG.info("NIOFetcher: finished at " + sdf.format(end) + ", elapsed: "
        + TimingUtil.elapsedTime(start, end));
  }

  /** Run the fetcher. */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new NIOFetcher(),
        args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {

    String usage = "Usage: NIOFetcher <segment>";

    if (args.length < 1) {
      System.err.println(usage);
      return -1;
    }

    Path segment = new Path(args[0]);

    try {
      fetch(segment);
      return 0;
    } catch (Exception e) {
      LOG.error("Fetcher: " + StringUtils.stringifyException(e));
      return -1;
    }

  }

  private void checkConfiguration() {
    // ensure that a value has been set for the agent name
    String agentName = getConf().get("http.agent.name");
    if (agentName == null || agentName.trim().length() == 0) {
      String message = "Fetcher: No agents listed in 'http.agent.name'"
          + " property.";
      if (LOG.isErrorEnabled()) {
        LOG.error(message);
      }
      throw new IllegalArgumentException(message);
    }
  }

  @Override
  public Map<String, Object> run(Map<String, Object> args, String crawlId)
      throws Exception {

    Map<String, Object> results = new HashMap<String, Object>();
    String RESULT = "result";
    String segment_dir = crawlId + "/segments";
    File segmentsDir = new File(segment_dir);
    File[] segmentsList = segmentsDir.listFiles();
    Arrays.sort(segmentsList, new Comparator<File>() {
      @Override
      public int compare(File f1, File f2) {
        if (f1.lastModified() > f2.lastModified())
          return -1;
        else
          return 0;
      }
    });

    Path segment = new Path(segmentsList[0].getPath());

    try {
      fetch(segment);
      results.put(RESULT, Integer.toString(0));
      return results;
    } catch (Exception e) {
      LOG.error("NIOFetcher: " + StringUtils.stringifyException(e));
      results.put(RESULT, Integer.toString(-1));
      return results;
    }
  }

}
