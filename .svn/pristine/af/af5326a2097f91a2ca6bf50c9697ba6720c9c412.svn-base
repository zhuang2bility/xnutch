package org.apache.nutch.crawl;

import java.io.*;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.*;

// Commons Logging imports
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.nutch.net.*;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TimingUtil;

/**
 * 从CrawlDb中提取链接主机并注入到HostDb。
 * 
 * @author kidden
 *
 */
public class HostInjector extends Configured implements Tool {
  public static final Logger LOG = LoggerFactory.getLogger(HostInjector.class);

  // TODO: 是否可以考虑从主机种子文件注入？若是，以下配置有用。
  // /** metadata key reserved for setting a custom score for a specific URL */
  // public static String nutchScoreMDName = "nutch.score";
  // /**
  // * metadata key reserved for setting a custom fetchInterval for a specific
  // URL
  // */
  // public static String nutchFetchIntervalMDName = "nutch.fetchInterval";
  // /**
  // * metadata key reserved for setting a fixed custom fetchInterval for a
  // * specific URL
  // */
  // public static String nutchFixedFetchIntervalMDName =
  // "nutch.fetchInterval.fixed";

  /** 过滤和规范化链接，提取链接主机 */
  public static class InjectMapper
      implements Mapper<Text, CrawlDatum, Text, CrawlDatum> {
    private URLNormalizers urlNormalizers;
    private int interval;
    // private float scoreInjected;
    private JobConf jobConf;
    private URLFilters filters;
    private ScoringFilters scfilters;
    private long curTime;

    public void configure(JobConf job) {
      this.jobConf = job;
      urlNormalizers = new URLNormalizers(job, URLNormalizers.SCOPE_INJECT);
      interval = jobConf.getInt("db.fetch.interval.default", 2592000);
      filters = new URLFilters(jobConf);
      scfilters = new ScoringFilters(jobConf);
      // scoreInjected = jobConf.getFloat("db.score.injected", 1.0f);
      curTime = job.getLong("injector.current.time",
          System.currentTimeMillis());
    }

    public void close() {
    }

    public void map(Text key, CrawlDatum value,
        OutputCollector<Text, CrawlDatum> output, Reporter reporter)
        throws IOException {
      String url = key.toString().trim(); // value is line of text

      if (url != null && (url.length() == 0 || url.startsWith("#"))) {
        /* Ignore line that start with # */
        return;
      }

      reporter.getCounter("HostInjector", "urls_scanned").increment(1);

      try {
        url = urlNormalizers.normalize(url, URLNormalizers.SCOPE_INJECT);
        url = filters.filter(url); // filter the url
      } catch (Exception e) {
        if (LOG.isWarnEnabled()) {
          LOG.warn("Skipping " + url + ":" + e);
        }
        url = null;
      }
      if (url == null) {
        reporter.getCounter("HostInjector", "urls_filtered").increment(1);
      } else { // if it passes
        String host = new URL(url).getHost();

        CrawlDatum datum = new CrawlDatum();
        datum.setStatus(CrawlDatum.STATUS_INJECTED);
        datum.setFetchInterval(interval);
        datum.setFetchTime(curTime);
        datum.setScore(value.getScore());

        try {
          scfilters.injectedScore(key, datum);
        } catch (ScoringFilterException e) {
          if (LOG.isWarnEnabled()) {
            LOG.warn("Cannot filter injected score for url " + url
                + ", using default (" + e.getMessage() + ")");
          }
        }

        output.collect(new Text(host), datum);
      }
    }
  }

  public static class InjectReducer
      implements Reducer<Text, CrawlDatum, Text, CrawlDatum> {

    public void configure(JobConf job) {
    }

    public void close() {
    }

    private CrawlDatum datum = new CrawlDatum();

    public void reduce(Text key, Iterator<CrawlDatum> values,
        OutputCollector<Text, CrawlDatum> output, Reporter reporter)
        throws IOException {
      float scores = 0;
      while (values.hasNext()) {
        // TODO: 分数和间隔从链接传递到主机
        CrawlDatum val = values.next();
        scores += val.getScore();
        datum.set(val);
      }
      datum.setScore(scores);
      output.collect(key, datum);
    }
  }

  // 合并InjectHost与HostDb的内容
  public static class MergeMapper
      implements Mapper<Text, CrawlDatum, Text, CrawlDatum> {

    public void configure(JobConf job) {
    }

    public void close() {
    }

    public void map(Text key, CrawlDatum value,
        OutputCollector<Text, CrawlDatum> output, Reporter reporter)
        throws IOException {

      output.collect(key, value);
    }
  }

  public static class MergeReducer
      implements Reducer<Text, CrawlDatum, Text, CrawlDatum> {
    private int interval;
    private boolean overwrite = false;
    private boolean update = false;

    public void configure(JobConf job) {
      // TODO: 主机的这些属性能和链接公用吗？
      interval = job.getInt("db.fetch.interval.default", 2592000);
      overwrite = job.getBoolean("hostdb.injector.overwrite", false);
      update = job.getBoolean("hostdb.injector.update", true);
      LOG.info("HostInjector: overwrite: " + overwrite);
      LOG.info("HostInjector: update: " + update);
    }

    public void close() {
    }

    private CrawlDatum old = new CrawlDatum();
    private CrawlDatum injected = new CrawlDatum();

    public void reduce(Text key, Iterator<CrawlDatum> values,
        OutputCollector<Text, CrawlDatum> output, Reporter reporter)
        throws IOException {
      boolean oldSet = false;
      boolean injectedSet = false;
      while (values.hasNext()) {
        CrawlDatum val = values.next();
        if (val.getStatus() == CrawlDatum.STATUS_INJECTED) {
          injected.set(val);
          injected.setStatus(CrawlDatum.STATUS_DB_UNFETCHED);
          injectedSet = true;
        } else {
          old.set(val);
          oldSet = true;
        }

      }

      CrawlDatum res = null;

      // Old default behaviour
      if (injectedSet && !oldSet) {
        res = injected;
      } else {
        res = old;
      }
      if (injectedSet && oldSet) {
        reporter.getCounter("HostInjector", "hosts_merged").increment(1);
      }
      /**
       * Whether to overwrite, ignore or update existing records
       * 
       * @see https://issues.apache.org/jira/browse/NUTCH-1405
       */
      // Injected record already exists and update but not overwrite
      if (injectedSet && oldSet && update && !overwrite) {
        res = old;
        old.putAllMetaData(injected);

        // old.setScore(injected.getScore() != scoreInjected ?
        // injected.getScore()
        // : old.getScore());
        // 每一次HostInjector都是遍历整个CrawlDb的操作，不是增量操作,所以直接覆盖
        old.setScore(injected.getScore());

        old.setFetchInterval(injected.getFetchInterval() != interval
            ? injected.getFetchInterval() : old.getFetchInterval());
      }

      // Injected record already exists and overwrite
      if (injectedSet && oldSet && overwrite) {
        res = injected;
      }

      output.collect(key, res);
    }
  }

  public HostInjector() {
  }

  public HostInjector(Configuration conf) {
    setConf(conf);
  }

  public void inject(Path crawlDb, Path hostDb) throws IOException {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    if (LOG.isInfoEnabled()) {
      LOG.info("HostInjector: starting at " + sdf.format(start));
      LOG.info("HostInjector: crawlDb: " + crawlDb);
      LOG.info("HostInjector: hostDb: " + hostDb);
    }

    Path tempDir = new Path(
        getConf().get("mapred.temp.dir", ".") + "/inject-temp-"
            + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    // map text input file to a <host,CrawlDatum> file
    if (LOG.isInfoEnabled()) {
      LOG.info("HostInjector: Converting urls to host entries.");
    }

    FileSystem fs = FileSystem.get(getConf());
    // determine if the hostdb already exists
    // boolean dbExists = fs.exists(hostDb);
    Path current = new Path(hostDb, HostDb.CURRENT_NAME);
    boolean dbExists = fs.exists(current);

    // 第一阶段MapReduce操作，将CrawlDb数据转换为<host,datum>，并计算host对应分数
    JobConf extractJob = new NutchJob(getConf());
    extractJob.setJobName("inject " + crawlDb);
    extractJob.setInputFormat(SequenceFileInputFormat.class);
    FileInputFormat.addInputPath(extractJob,
        new Path(crawlDb, CrawlDb.CURRENT_NAME));
    extractJob.setMapperClass(InjectMapper.class);
    extractJob.setReducerClass(InjectReducer.class);
    extractJob.setOutputFormat(MapFileOutputFormat.class);
    FileOutputFormat.setOutputPath(extractJob, tempDir);
    // if (dbExists) {// 如果hostdb存在,则不合并urls,等待与old hostdb合并
    // // Don't run merge injected urls, wait for merge with
    // // existing DB
    // extractJob.setOutputFormat(SequenceFileOutputFormat.class);
    // extractJob.setNumReduceTasks(0);
    // } else {// hostdb不存在,则合并urls
    // extractJob.setOutputFormat(MapFileOutputFormat.class);
    // // extractJob.setReducerClass(MergeReducer.class);
    // extractJob.setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs",
    // false);
    // }
    extractJob.setOutputKeyClass(Text.class);
    extractJob.setOutputValueClass(CrawlDatum.class);
    extractJob.setLong("injector.current.time", System.currentTimeMillis());

    RunningJob mapJob = null;
    try {
      mapJob = JobClient.runJob(extractJob);
    } catch (IOException e) {
      fs.delete(tempDir, true);
      throw e;
    }
    long urlsInjected = mapJob.getCounters()
        .findCounter("HostInjector", "urls_scanned").getValue();
    long urlsFiltered = mapJob.getCounters()
        .findCounter("HostInjector", "urls_filtered").getValue();
    // long hostsInjected = mapJob.getCounters()
    // .findCounter("HostInjector", "hosts_extracted").getValue();
    LOG.info(
        "Injector: Total number of urls rejected by filters: " + urlsFiltered);
    LOG.info("Injector: Total number of urls scanned: " + urlsInjected);
    // LOG.info("Injector: Total number of hosts injected: " + hostsInjected);

    // 第二阶段MapReduce操作，如果存在hostDb则合并injectHost与HostDb
    long hostsMerged = 0;

    if (dbExists) {
      // merge with existing hostdb
      if (LOG.isInfoEnabled()) {
        LOG.info("HostInjector: Merging extracted hosts into host db.");
      }
      JobConf mergeJob = HostDb.createJob(getConf(), hostDb);

      // 合并injectHost与hostDb中的内容，保存已解析的IP
      FileInputFormat.addInputPath(mergeJob, tempDir);
      FileInputFormat.addInputPath(mergeJob,
          new Path(hostDb, HostDb.CURRENT_NAME));
      mergeJob.setMapperClass(MergeMapper.class);
      mergeJob.setReducerClass(MergeReducer.class);
      try {
        RunningJob merge = JobClient.runJob(mergeJob);
        hostsMerged = merge.getCounters()
            .findCounter("HostInjector", "hosts_merged").getValue();
        LOG.info("HostInjector: HOSTs merged: " + hostsMerged);
      } catch (IOException e) {
        fs.delete(tempDir, true);
        throw e;
      }
      HostDb.install(mergeJob, hostDb);
    } else {
      HostDb.install(extractJob, hostDb);
    }

    // clean up
    fs.delete(tempDir, true);
    // LOG.info("Injector: Total new urls injected: "
    // + (urlsInjected - hostsMerged));
    long end = System.currentTimeMillis();
    LOG.info("HostInjector: finished at " + sdf.format(end) + ", elapsed: "
        + TimingUtil.elapsedTime(start, end));
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new HostInjector(),
        args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: HostInjector <crawldb> <hostdb>");
      return -1;
    }
    try {
      inject(new Path(args[0]), new Path(args[1]));
      return 0;
    } catch (Exception e) {
      LOG.error("HostInjector: " + StringUtils.stringifyException(e));
      return -1;
    }
  }

}
