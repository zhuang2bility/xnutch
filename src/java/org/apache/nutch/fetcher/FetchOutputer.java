package org.apache.nutch.fetcher;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.crawl.SignatureFactory;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.URLFilterException;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.parse.ParseOutputFormat;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.parse.ParseSegment;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.parse.ParseText;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.ProtocolStatus;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.URLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FetchOutputer {
  private static final Logger LOG = LoggerFactory.getLogger(FetchOutputer.class);
  
  private Configuration conf;
  private URLFilters urlFilters;
  private ScoringFilters scfilters;
  private ParseUtil parseUtil;
  private URLNormalizers normalizers;
//  private ProtocolFactory protocolFactory;
//  private long maxCrawlDelay;
  private String queueMode;
  private int maxRedirect;
//  private String reprUrl;
//  private boolean redirecting;
//  private int redirectCount;
  private RedirectInfo redirectInfo;
  private boolean ignoreExternalLinks;
  private String ignoreExternalLinksMode;

  // Used by fetcher.follow.outlinks.depth in parse
  private int maxOutlinksPerPage;
  private final int maxOutlinks;
  private final int interval;
  private int maxOutlinkDepth;
  private int maxOutlinkDepthNumLinks;
  private boolean outlinksIgnoreExternal;

  private int outlinksDepthDivisor;
  private boolean skipTruncated;

//  private boolean halted = false;
//
//  private AtomicInteger activeThreads;

  private Object fetchQueues;

//  private QueueFeeder feeder;
//
//  private Object spinWaiting;
//
//  private AtomicLong lastRequestStart;

  private Reporter reporter;

//  private AtomicInteger errors;

  private String segmentName;

  private boolean parsing;

  private OutputCollector<Text, NutchWritable> output;

  private boolean storingContent;

//  private AtomicInteger pages;
//
//  private AtomicLong bytes;
  
  //Used by the REST service
  private FetchNode fetchNode;
//  private boolean reportToNutchServer;
  
  public FetchOutputer(Configuration conf, FetchItemQueues fetchQueues, 
      Reporter reporter, String segmentName, boolean parsing, OutputCollector<Text, NutchWritable> output,
      boolean storingContent, RedirectInfo redirectInfo, FetchNode fetchNode){
    this.conf = conf;
    this.urlFilters = new URLFilters(conf);
    this.scfilters = new ScoringFilters(conf);
    this.parseUtil = new ParseUtil(conf);
    this.skipTruncated = conf.getBoolean(ParseSegment.SKIP_TRUNCATED, true);
//    this.protocolFactory = new ProtocolFactory(conf);
    this.normalizers = new URLNormalizers(conf, URLNormalizers.SCOPE_FETCHER);
//    this.maxCrawlDelay = conf.getInt("fetcher.max.crawl.delay", 30) * 1000;
//    this.activeThreads = activeThreads;
    this.fetchQueues = fetchQueues;
//    this.feeder = feeder;
//    this.spinWaiting = spinWaiting;
//    this.lastRequestStart = lastRequestStart;
    this.reporter = reporter;
//    this.errors = errors;
    this.segmentName = segmentName;
    this.parsing = parsing;
    this.output = output;
    this.storingContent = storingContent;
//    this.pages = pages;
//    this.bytes = bytes;
    queueMode = conf.get("fetcher.queue.mode",
        FetchItemQueues.QUEUE_MODE_HOST);
    // check that the mode is known
    if (!queueMode.equals(FetchItemQueues.QUEUE_MODE_IP)
        && !queueMode.equals(FetchItemQueues.QUEUE_MODE_DOMAIN)
        && !queueMode.equals(FetchItemQueues.QUEUE_MODE_HOST)) {
      LOG.error("Unknown partition mode : " + queueMode
          + " - forcing to byHost");
      queueMode = FetchItemQueues.QUEUE_MODE_HOST;
    }
    LOG.info("Using queue mode : " + queueMode);
    this.maxRedirect = conf.getInt("http.redirect.max", 3);

    maxOutlinksPerPage = conf.getInt("db.max.outlinks.per.page", 100);
    maxOutlinks = (maxOutlinksPerPage < 0) ? Integer.MAX_VALUE
        : maxOutlinksPerPage;
    interval = conf.getInt("db.fetch.interval.default", 2592000);
    ignoreExternalLinks = conf.getBoolean("db.ignore.external.links", false);
    ignoreExternalLinksMode = conf.get("db.ignore.external.links.mode", "byHost");
    maxOutlinkDepth = conf.getInt("fetcher.follow.outlinks.depth", -1);
    outlinksIgnoreExternal = conf.getBoolean(
        "fetcher.follow.outlinks.ignore.external", false);
    maxOutlinkDepthNumLinks = conf.getInt(
        "fetcher.follow.outlinks.num.links", 4);
    outlinksDepthDivisor = conf.getInt(
        "fetcher.follow.outlinks.depth.divisor", 2);
    
    this.redirectInfo = redirectInfo;
    this.fetchNode = fetchNode;
  }
  
  public Text handleRedirect(Text url, CrawlDatum datum, String urlString,
      String newUrl, boolean temp, String redirType)
      throws MalformedURLException, URLFilterException {
    newUrl = normalizers.normalize(newUrl, URLNormalizers.SCOPE_FETCHER);
    newUrl = urlFilters.filter(newUrl);

    if (ignoreExternalLinks) {
      try {
        String origHost = new URL(urlString).getHost().toLowerCase();
        String newHost = new URL(newUrl).getHost().toLowerCase();
        if (!origHost.equals(newHost)) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(" - ignoring redirect " + redirType + " from "
                + urlString + " to " + newUrl
                + " because external links are ignored");
          }
          return null;
        }
      } catch (MalformedURLException e) {
      }
    }

    if (newUrl != null && !newUrl.equals(urlString)) {
      redirectInfo.reprUrl = URLUtil.chooseRepr(redirectInfo.reprUrl, newUrl, temp);
      url = new Text(newUrl);
      if (maxRedirect > 0) {
//        redirecting = true;
//        redirectCount++;
        redirectInfo.redirecting = true;
        redirectInfo.redirectCount++;
        if (LOG.isDebugEnabled()) {
          LOG.debug(" - " + redirType + " redirect to " + url
              + " (fetching now)");
        }
        return url;
      } else {
        CrawlDatum newDatum = new CrawlDatum(CrawlDatum.STATUS_LINKED,
            datum.getFetchInterval(), datum.getScore());
        // transfer existing metadata
        newDatum.getMetaData().putAll(datum.getMetaData());
        try {
          scfilters.initialScore(url, newDatum);
        } catch (ScoringFilterException e) {
          e.printStackTrace();
        }
        if (redirectInfo.reprUrl != null) {
          newDatum.getMetaData().put(Nutch.WRITABLE_REPR_URL_KEY,
              new Text(redirectInfo.reprUrl));
        }
        output(url, newDatum, null, null, CrawlDatum.STATUS_LINKED);
        if (LOG.isDebugEnabled()) {
          LOG.debug(" - " + redirType + " redirect to " + url
              + " (fetching later)");
        }
        return null;
      }
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug(" - " + redirType + " redirect skipped: "
            + (newUrl != null ? "to same url" : "filtered"));
      }
      return null;
    }
  }

  public FetchItem queueRedirect(Text redirUrl, FetchItem fit)
      throws ScoringFilterException {
    CrawlDatum newDatum = new CrawlDatum(CrawlDatum.STATUS_DB_UNFETCHED,
        fit.datum.getFetchInterval(), fit.datum.getScore());
    // transfer all existing metadata to the redirect
    newDatum.getMetaData().putAll(fit.datum.getMetaData());
    scfilters.initialScore(redirUrl, newDatum);
    if (redirectInfo.reprUrl != null) {
      newDatum.getMetaData().put(Nutch.WRITABLE_REPR_URL_KEY,
          new Text(redirectInfo.reprUrl));
    }
    fit = FetchItem.create(redirUrl, newDatum, queueMode);
    if (fit != null) {
      FetchItemQueue fiq = ((FetchItemQueues) fetchQueues).getFetchItemQueue(fit.queueID);
      fiq.addInProgressFetchItem(fit);
    } else {
      // stop redirecting
//      redirecting = false;
      redirectInfo.redirecting = false;
      reporter.incrCounter("FetcherStatus", "FetchItem.notCreated.redirect",
          1);
    }
    return fit;
  }

  public ParseStatus output(Text key, CrawlDatum datum, Content content,
      ProtocolStatus pstatus, int status) {

    return output(key, datum, content, pstatus, status, 0);
  }
  
  public ParseStatus output(Text key, CrawlDatum datum, Content content,
      ProtocolStatus pstatus, int status, int outlinkDepth) {

    datum.setStatus(status);
    datum.setFetchTime(System.currentTimeMillis());
    if (pstatus != null)
      datum.getMetaData().put(Nutch.WRITABLE_PROTO_STATUS_KEY, pstatus);

    ParseResult parseResult = null;
    if (content != null) {
      Metadata metadata = content.getMetadata();

      // store the guessed content type in the crawldatum
      if (content.getContentType() != null)
        datum.getMetaData().put(new Text(Metadata.CONTENT_TYPE),
            new Text(content.getContentType()));

      // add segment to metadata
      metadata.set(Nutch.SEGMENT_NAME_KEY, segmentName);
      // add score to content metadata so that ParseSegment can pick it up.
      try {
        scfilters.passScoreBeforeParsing(key, datum, content);
      } catch (Exception e) {
        if (LOG.isWarnEnabled()) {
          LOG.warn("Couldn't pass score, url " + key + " (" + e + ")");
        }
      }
      /*
       * Note: Fetcher will only follow meta-redirects coming from the
       * original URL.
       */
      if (parsing && status == CrawlDatum.STATUS_FETCH_SUCCESS) {
        if (!skipTruncated
            || (skipTruncated && !ParseSegment.isTruncated(content))) {
          try {
            parseResult = this.parseUtil.parse(content);
          } catch (Exception e) {
            LOG.warn("Error parsing: " + key + ": "
                + StringUtils.stringifyException(e));
          }
        }

        if (parseResult == null) {
          byte[] signature = SignatureFactory.getSignature(conf)
              .calculate(content, new ParseStatus().getEmptyParse(conf));
          datum.setSignature(signature);
        }
      }

      /*
       * Store status code in content So we can read this value during parsing
       * (as a separate job) and decide to parse or not.
       */
      content.getMetadata().add(Nutch.FETCH_STATUS_KEY,
          Integer.toString(status));
    }

    try {
      output.collect(key, new NutchWritable(datum));
      if (content != null && storingContent)
        output.collect(key, new NutchWritable(content));
      if (parseResult != null) {
        for (Entry<Text, Parse> entry : parseResult) {
          Text url = entry.getKey();
          Parse parse = entry.getValue();
          ParseStatus parseStatus = parse.getData().getStatus();
          ParseData parseData = parse.getData();

          if (!parseStatus.isSuccess()) {
            LOG.warn("Error parsing: " + key + ": " + parseStatus);
            parse = parseStatus.getEmptyParse(conf);
          }

          // Calculate page signature. For non-parsing fetchers this will
          // be done in ParseSegment
          byte[] signature = SignatureFactory.getSignature(conf)
              .calculate(content, parse);
          // Ensure segment name and score are in parseData metadata
          parseData.getContentMeta().set(Nutch.SEGMENT_NAME_KEY, segmentName);
          parseData.getContentMeta().set(Nutch.SIGNATURE_KEY,
              StringUtil.toHexString(signature));
          // Pass fetch time to content meta
          parseData.getContentMeta().set(Nutch.FETCH_TIME_KEY,
              Long.toString(datum.getFetchTime()));
          if (url.equals(key))
            datum.setSignature(signature);
          try {
            scfilters.passScoreAfterParsing(url, content, parse);
          } catch (Exception e) {
            if (LOG.isWarnEnabled()) {
              LOG.warn("Couldn't pass score, url " + key + " (" + e + ")");
            }
          }

          String origin = null;

          // collect outlinks for subsequent db update
          Outlink[] links = parseData.getOutlinks();
          int outlinksToStore = Math.min(maxOutlinks, links.length);
          if (ignoreExternalLinks) {
            URL originURL = new URL(url.toString());
            // based on domain?
            if ("bydomain".equalsIgnoreCase(ignoreExternalLinksMode)) {
              origin = URLUtil.getDomainName(originURL).toLowerCase();
            } 
            // use host 
            else {
              origin = originURL.getHost().toLowerCase();
            }
          }
          
          //used by fetchNode         
          if(fetchNode!=null){
            fetchNode.setOutlinks(links);
            fetchNode.setTitle(parseData.getTitle());
            FetchNodeDb.getInstance().put(fetchNode.getUrl().toString(), fetchNode);
          }
          int validCount = 0;

          // Process all outlinks, normalize, filter and deduplicate
          List<Outlink> outlinkList = new ArrayList<Outlink>(outlinksToStore);
          HashSet<String> outlinks = new HashSet<String>(outlinksToStore);
          for (int i = 0; i < links.length && validCount < outlinksToStore; i++) {
            String toUrl = links[i].getToUrl();

            toUrl = ParseOutputFormat.filterNormalize(url.toString(), toUrl,
                origin, ignoreExternalLinks, ignoreExternalLinksMode, urlFilters, normalizers);
            if (toUrl == null) {
              continue;
            }

            validCount++;
            links[i].setUrl(toUrl);
            outlinkList.add(links[i]);
            outlinks.add(toUrl);
          }

          // Only process depth N outlinks
          if (maxOutlinkDepth > 0 && outlinkDepth < maxOutlinkDepth) {
            reporter.incrCounter("FetcherOutlinks", "outlinks_detected",
                outlinks.size());

            // Counter to limit num outlinks to follow per page
            int outlinkCounter = 0;

            // Calculate variable number of outlinks by depth using the
            // divisor (outlinks = Math.floor(divisor / depth * num.links))
            int maxOutlinksByDepth = (int) Math.floor(outlinksDepthDivisor
                / (outlinkDepth + 1) * maxOutlinkDepthNumLinks);

            String followUrl;

            // Walk over the outlinks and add as new FetchItem to the queues
            Iterator<String> iter = outlinks.iterator();
            while (iter.hasNext() && outlinkCounter < maxOutlinkDepthNumLinks) {
              followUrl = iter.next();

              // Check whether we'll follow external outlinks
              if (outlinksIgnoreExternal) {
                if (!URLUtil.getHost(url.toString()).equals(
                    URLUtil.getHost(followUrl))) {
                  continue;
                }
              }

              reporter
                  .incrCounter("FetcherOutlinks", "outlinks_following", 1);

              // Create new FetchItem with depth incremented
              FetchItem fit = FetchItem.create(new Text(followUrl),
                  new CrawlDatum(CrawlDatum.STATUS_LINKED, interval),
                  queueMode, outlinkDepth + 1);
              ((FetchItemQueues) fetchQueues).addFetchItem(fit);

              outlinkCounter++;
            }
          }

          // Overwrite the outlinks in ParseData with the normalized and
          // filtered set
          parseData.setOutlinks(outlinkList.toArray(new Outlink[outlinkList
              .size()]));

          output.collect(url, new NutchWritable(new ParseImpl(new ParseText(
              parse.getText()), parseData, parse.isCanonical())));
        }
      }
    } catch (IOException e) {
      if (LOG.isErrorEnabled()) {
        LOG.error("fetcher caught:" + e.toString());
      }
    }

    // return parse status if it exits
    if (parseResult != null && !parseResult.isEmpty()) {
      Parse p = parseResult.get(content.getUrl());
      if (p != null) {
        reporter.incrCounter("ParserStatus", ParseStatus.majorCodes[p
            .getData().getStatus().getMajorCode()], 1);
        return p.getData().getStatus();
      }
    }
    return null;
  }
}
