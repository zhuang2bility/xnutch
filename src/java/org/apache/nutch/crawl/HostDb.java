

package org.apache.nutch.crawl;

import java.io.*;
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

import org.apache.nutch.util.HadoopFSUtil;
import org.apache.nutch.util.LockUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TimingUtil;

/*
 * 主机信息库更新。
 * 
 * @author kidden
 *
 */
public class HostDb extends Configured implements Tool {
  public static final Logger LOG = LoggerFactory.getLogger(HostDb.class);

  public static final String CRAWLDB_ADDITIONS_ALLOWED = "db.update.additions.allowed";

  public static final String CRAWLDB_PURGE_404 = "db.update.purge.404";

  public static final String CURRENT_NAME = "current";
  
  public static final String LOCK_NAME = ".locked";
  
  public HostDb() {}
  
  public HostDb(Configuration conf) {
    setConf(conf);
  }

  public void update(Path hostDb, Path[] segments) throws IOException {
    boolean additionsAllowed = getConf().getBoolean(CRAWLDB_ADDITIONS_ALLOWED, true);
    update(hostDb, segments, additionsAllowed, false);
  }
  
  public void update(Path hostDb, Path[] segments, boolean additionsAllowed, boolean force) throws IOException {
    FileSystem fs = FileSystem.get(getConf());
    Path lock = new Path(hostDb, LOCK_NAME);
    LockUtil.createLockFile(fs, lock, force);
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();

    JobConf job = HostDb.createJob(getConf(), hostDb);
    job.setBoolean(CRAWLDB_ADDITIONS_ALLOWED, additionsAllowed);

    boolean url404Purging = job.getBoolean(CRAWLDB_PURGE_404, false);

    if (LOG.isInfoEnabled()) {
      LOG.info("HostDb update: starting at " + sdf.format(start));
      LOG.info("HostDb update: db: " + hostDb);
      LOG.info("HostDb update: segments: " + Arrays.asList(segments));
      LOG.info("HostDb update: additions allowed: " + additionsAllowed);
      LOG.info("HostDb update: 404 purging: " + url404Purging);
    }

    for (int i = 0; i < segments.length; i++) {
      Path fetch = new Path(segments[i], CrawlDatum.FETCH_DIR_NAME);
      if (fs.exists(fetch)) {
        FileInputFormat.addInputPath(job, fetch);
      } else {
        LOG.info(" - skipping invalid segment " + segments[i]);
      }
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("HostDb update: Merging segment data into db.");
    }
    try {
      JobClient.runJob(job);
    } catch (IOException e) {
      LockUtil.removeLockFile(fs, lock);
      Path outPath = FileOutputFormat.getOutputPath(job);
      if (fs.exists(outPath) ) fs.delete(outPath, true);
      throw e;
    }

    HostDb.install(job, hostDb);
    long end = System.currentTimeMillis();
    LOG.info("HostDb update: finished at " + sdf.format(end) + ", elapsed: " + TimingUtil.elapsedTime(start, end));
  }

  public static JobConf createJob(Configuration config, Path hostDb)
    throws IOException {
    Path newHostDb =
      new Path(hostDb,
               Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    JobConf job = new NutchJob(config);
    job.setJobName("hostdb " + hostDb);

    Path current = new Path(hostDb, CURRENT_NAME);
    if (FileSystem.get(job).exists(current)) {
      FileInputFormat.addInputPath(job, current);
    }
    job.setInputFormat(SequenceFileInputFormat.class);

//    job.setMapperClass(CrawlDbFilter.class);
    job.setReducerClass(CrawlDbReducer.class);

    FileOutputFormat.setOutputPath(job, newHostDb);
    job.setOutputFormat(MapFileOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(CrawlDatum.class);

    // https://issues.apache.org/jira/browse/NUTCH-1110
    job.setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", false);

    return job;
  }

  public static void install(JobConf job, Path hostDb) throws IOException {
    boolean preserveBackup = job.getBoolean("db.preserve.backup", true);

    Path newCrawlDb = FileOutputFormat.getOutputPath(job);
    FileSystem fs = new JobClient(job).getFs();
    Path old = new Path(hostDb, "old");
    Path current = new Path(hostDb, CURRENT_NAME);
    if (fs.exists(current)) {
      if (fs.exists(old)) fs.delete(old, true);
      fs.rename(current, old);
    }
    fs.mkdirs(hostDb);
    fs.rename(newCrawlDb, current);
    if (!preserveBackup && fs.exists(old)) fs.delete(old, true);
    Path lock = new Path(hostDb, LOCK_NAME);
    LockUtil.removeLockFile(fs, lock);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new HostDb(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println("Usage: HostDb <hostdb> (-dir <segments> | <seg1> <seg2> ...) [-force] [-noAdditions]");
      System.err.println("\thostdb\tHostDb to update");
      System.err.println("\t-dir segments\tparent directory containing all segments to update from");
      System.err.println("\tseg1 seg2 ...\tlist of segment names to update from");
      System.err.println("\t-force\tforce update even if HostDb appears to be locked (CAUTION advised)");
      System.err.println("\t-noAdditions\tonly update already existing HOSTs, don't add any newly discovered HOSTs");

      return -1;
    }

    boolean additionsAllowed = getConf().getBoolean(CRAWLDB_ADDITIONS_ALLOWED, true);
    boolean force = false;
    final FileSystem fs = FileSystem.get(getConf());
    HashSet<Path> dirs = new HashSet<Path>();
    for (int i = 1; i < args.length; i++) {
       if (args[i].equals("-force")) {
        force = true;
      } else if (args[i].equals("-noAdditions")) {
        additionsAllowed = false;
      } else if (args[i].equals("-dir")) {
        FileStatus[] paths = fs.listStatus(new Path(args[++i]), HadoopFSUtil.getPassDirectoriesFilter(fs));
        dirs.addAll(Arrays.asList(HadoopFSUtil.getPaths(paths)));
      } else {
        dirs.add(new Path(args[i]));
      }
    }
    try {
      update(new Path(args[0]), dirs.toArray(new Path[dirs.size()]), additionsAllowed, force);
      return 0;
    } catch (Exception e) {
      LOG.error("HostDb update: " + StringUtils.stringifyException(e));
      return -1;
    }
  }
}
