/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.storage.hdfs;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.Module;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.multibindings.MapBinder;

import io.druid.data.SearchableVersionedDataFinder;
import io.druid.guice.Binders;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.LifecycleModule;
import io.druid.guice.ManageLifecycle;
import io.druid.initialization.DruidModule;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.storage.hdfs.tasklog.HdfsTaskLogs;
import io.druid.storage.hdfs.tasklog.HdfsTaskLogsConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 */
public class HdfsStorageDruidModule implements DruidModule
{
  private static final Logger log = new Logger(HdfsStorageDruidModule.class);
  public static final String SCHEME = "hdfs";
  private Properties props = null;

  @Inject
  public void setProperties(Properties props)
  {
    this.props = props;
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new Module()
        {
          @Override
          public String getModuleName()
          {
            return "DruidHDFSStorage-" + System.identityHashCode(this);
          }

          @Override
          public Version version()
          {
            return Version.unknownVersion();
          }

          @Override
          public void setupModule(SetupContext context)
          {
            context.registerSubtypes(HdfsLoadSpec.class);
          }
        }
    );
  }

  public static void printJVMPaths(String tag, Logger loge)
  {
    try {
      loge.error(tag);
      RuntimeMXBean bean = ManagementFactory.getRuntimeMXBean();
      List<String> jvmArgs = bean.getInputArguments();

      for (int i = 0; i < jvmArgs.size(); i++) {
        loge.error(jvmArgs.get(i));
      }

      loge.error((" -classpath " + System.getProperty("java.class.path")));
      // print the non-JVM command line arguments
      // print name of the main class with its arguments, like org.ClassName param1 param2
      loge.error(" " + System.getProperty("sun.java.command"));

      String[] classpaths = System.getProperty("java.class.path").split(":");
      if (classpaths.length > 0) {
        if (classpaths[0].contains("stg_renaissance1")) {
          File jobxml = new File(classpaths[0] + "/job.xml");
          String filename = classpaths[0] + "/job.xml";
          try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String line = null;
            while ((line = br.readLine()) != null) {
              loge.error("job.xml" + "[" + line + "]");
            }
          }
        } else {
          loge.error("not stg_renaissance1");
        }
      } else {
        loge.error("no classpath splits");
      }
    }
    catch (Exception e) {
      loge.error(e.getMessage());
    }
  }

  public static void printConfig(String tag, Configuration configuration, Logger loge)
  {
    Iterator<Map.Entry<String, String>> entryIterator = configuration.iterator();
    List<String> configKeyValues = new ArrayList<>();
    while (entryIterator.hasNext()) {
      Map.Entry < String, String > ent = entryIterator.next();
      String keyVal = StringUtils.format("%s[%s : %s]", tag, ent.getKey(), ent.getValue());
      configKeyValues.add(keyVal);
    }

    String bigString = "";
    configKeyValues.sort(Ordering.natural());
    for (String keyVal : configKeyValues) {
      if (loge != null) {
        loge.error(keyVal);
      } else {
        System.out.println(keyVal);
      }
      bigString += "\n" + keyVal;
    }
  }

  @Override
  public void configure(Binder binder)
  {
    MapBinder.newMapBinder(binder, String.class, SearchableVersionedDataFinder.class)
             .addBinding(SCHEME)
             .to(HdfsFileTimestampVersionFinder.class)
             .in(LazySingleton.class);

    Binders.dataSegmentPullerBinder(binder).addBinding(SCHEME).to(HdfsDataSegmentPuller.class).in(LazySingleton.class);
    Binders.dataSegmentPusherBinder(binder).addBinding(SCHEME).to(HdfsDataSegmentPusher.class).in(LazySingleton.class);
    Binders.dataSegmentKillerBinder(binder).addBinding(SCHEME).to(HdfsDataSegmentKiller.class).in(LazySingleton.class);
    Binders.dataSegmentFinderBinder(binder).addBinding(SCHEME).to(HdfsDataSegmentFinder.class).in(LazySingleton.class);

    final Configuration conf = new Configuration();

    printJVMPaths("HdfsStorageModule", log);
    printConfig("HdfsStorageModule1", conf, log);

    // Set explicit CL. Otherwise it'll try to use thread context CL, which may not have all of our dependencies.
    conf.setClassLoader(getClass().getClassLoader());

    // Ensure that FileSystem class level initialization happens with correct CL
    // See https://github.com/druid-io/druid/issues/1714
    ClassLoader currCtxCl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
      FileSystem.get(conf);
    }
    catch (IOException ex) {
      throw Throwables.propagate(ex);
    }
    finally {
      Thread.currentThread().setContextClassLoader(currCtxCl);
    }

    printConfig("HdfsStorageModule2", conf, log);

    if (props != null) {
      for (String propName : props.stringPropertyNames()) {
        if (propName.startsWith("hadoop.")) {
          conf.set(propName.substring("hadoop.".length()), props.getProperty(propName));
        }
      }
    }

    binder.bind(Configuration.class).toInstance(conf);
    JsonConfigProvider.bind(binder, "druid.storage", HdfsDataSegmentPusherConfig.class);

    Binders.taskLogsBinder(binder).addBinding("hdfs").to(HdfsTaskLogs.class);
    JsonConfigProvider.bind(binder, "druid.indexer.logs", HdfsTaskLogsConfig.class);
    binder.bind(HdfsTaskLogs.class).in(LazySingleton.class);
    JsonConfigProvider.bind(binder, "druid.hadoop.security.kerberos", HdfsKerberosConfig.class);
    binder.bind(HdfsStorageAuthentication.class).in(ManageLifecycle.class);
    LifecycleModule.register(binder, HdfsStorageAuthentication.class);

  }
}
