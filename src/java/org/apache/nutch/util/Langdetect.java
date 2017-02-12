package org.apache.nutch.util;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.JarURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * Created by zhangmingke on 16/7/11.
 */
public class Langdetect {
    public static Logger LOG = LoggerFactory.getLogger(Langdetect.class);

    private static boolean isLoad  = false;
    private static List<String> profiles = null;

    static {
        if (!isLoad) {
            try {
                loadProfiles();
                isLoad = true;
            } catch (IOException e) {
                LOG.info(e.getMessage());
            }

        }
    }

    protected static void loadProfiles() throws IOException {
        LOG.info("load again" + "\n");
        String dirname = "profiles/";
        Enumeration<URL> en = Detector.class.getClassLoader().getResources(
                dirname);
        profiles = new ArrayList<>();
        if (en.hasMoreElements()) {
            URL url = en.nextElement();
            JarURLConnection urlcon = (JarURLConnection) url.openConnection();
            try (JarFile jar = urlcon.getJarFile();) {
                Enumeration<JarEntry> entries = jar.entries();
                while (entries.hasMoreElements()) {
                    String entry = entries.nextElement().getName();
                    if (entry.startsWith(dirname)) {
                        try (InputStream in = Detector.class.getClassLoader()
                                .getResourceAsStream(entry);) {
                            profiles.add(IOUtils.toString(in));
                        }
                    }
                }
            }
        }
        try {
            DetectorFactory.loadProfile(profiles);
        } catch (LangDetectException e) {
            e.printStackTrace();
        }
    }

    public static String detect(String text) {
        Detector detector = null;
        try {
            detector = DetectorFactory.create();
        } catch (LangDetectException e) {
            e.printStackTrace();
        }
        detector.append(text);
        String language = null;
        try {
            LOG.info("load zmk");
            language = detector.detect();
        } catch (LangDetectException e) {
            e.printStackTrace();
        }
        return language;
    }

}
