/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.client.naming.backups;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.client.naming.cache.ConcurrentDiskUtil;
import com.alibaba.nacos.client.naming.cache.DiskCache;
import com.alibaba.nacos.client.naming.cache.ServiceInfoHolder;
import com.alibaba.nacos.client.naming.utils.CollectionUtils;
import com.alibaba.nacos.client.naming.utils.UtilAndComs;
import com.alibaba.nacos.common.lifecycle.Closeable;
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.common.utils.ThreadUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.StringReader;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.alibaba.nacos.client.utils.LogUtils.NAMING_LOGGER;

/**
 * Failover reactor.
 *
 * @author nkorange
 */
public class FailoverReactor implements Closeable {
    
    private static final String FAILOVER_DIR = "/failover";
    
    private static final String IS_FAILOVER_MODE = "1";
    
    private static final String NO_FAILOVER_MODE = "0";
    
    private static final String FAILOVER_MODE_PARAM = "failover-mode";
    
    private Map<String, ServiceInfo> serviceMap = new ConcurrentHashMap<>();
    
    private final Map<String, String> switchParams = new ConcurrentHashMap<>();
    
    private static final long DAY_PERIOD_MINUTES = 24 * 60;
    
    private final String failoverDir;
    
    private final ServiceInfoHolder serviceInfoHolder;
    
    private final ScheduledExecutorService executorService;
    
    public FailoverReactor(ServiceInfoHolder serviceInfoHolder, String cacheDir) {
        // 持有ServiceInfoHolder引用
        this.serviceInfoHolder = serviceInfoHolder;
        // 拼接故障根目录：${user.home}/nacos/naming/public/failover
        this.failoverDir = cacheDir + FAILOVER_DIR;
        // 初始化executorService
        this.executorService = new ScheduledThreadPoolExecutor(1, r -> {
            Thread thread = new Thread(r);
            // 守护线程模式运行
            thread.setDaemon(true);
            thread.setName("com.alibaba.nacos.naming.failover");
            return thread;
        });
        // 其他初始化操作，通过executorService开启多个定时任务执行
        this.init();
    }
    
    /**
     * Init.
     */
    public void init() {

        // 初始化立即执行，执行间隔5秒，执行任务为SwitchRefresher
        executorService.scheduleWithFixedDelay(new SwitchRefresher(), 0L, 5000L, TimeUnit.MILLISECONDS);

        // 初始化延迟30分钟执行，执行间隔24小时，执行任务为DiskFileWriter
        executorService.scheduleWithFixedDelay(new DiskFileWriter(), 30, DAY_PERIOD_MINUTES, TimeUnit.MINUTES);
        
        // 初始化立即执行，执行间隔10秒，执行核心操作为DiskFileWriter
        executorService.schedule(() -> {
            try {
                File cacheDir = new File(failoverDir);

                if (!cacheDir.exists() && !cacheDir.mkdirs()) {
                    throw new IllegalStateException("failed to create cache dir: " + failoverDir);
                }

                File[] files = cacheDir.listFiles();
                if (files == null || files.length <= 0) {
                    new DiskFileWriter().run();
                }
            } catch (Throwable e) {
                NAMING_LOGGER.error("[NA] failed to backup file on startup.", e);
            }

        }, 10000L, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Add day.
     *
     * @param date start time
     * @param num  add day number
     * @return new date
     */
    public Date addDay(Date date, int num) {
        Calendar startDT = Calendar.getInstance();
        startDT.setTime(date);
        startDT.add(Calendar.DAY_OF_MONTH, num);
        return startDT.getTime();
    }
    
    @Override
    public void shutdown() throws NacosException {
        String className = this.getClass().getName();
        NAMING_LOGGER.info("{} do shutdown begin", className);
        ThreadUtils.shutdownThreadPool(executorService, NAMING_LOGGER);
        NAMING_LOGGER.info("{} do shutdown stop", className);
    }
    
    class SwitchRefresher implements Runnable {
        
        long lastModifiedMillis = 0L;
        
        @Override
        public void run() {
            try {
                File switchFile = Paths.get(failoverDir, UtilAndComs.FAILOVER_SWITCH).toFile();
                //  如果故障转移文件不存在，则直接返回
                if (!switchFile.exists()) {
                    switchParams.put(FAILOVER_MODE_PARAM, Boolean.FALSE.toString());
                    NAMING_LOGGER.debug("failover switch is not found, {}", switchFile.getName());
                    return;
                }
                
                long modified = switchFile.lastModified();
                // 比较文件修改时间，如果已经修改，则获取故障转移文件中的内容
                if (lastModifiedMillis < modified) {
                    lastModifiedMillis = modified;
                    // 获取故障转移文件内容
                    String failover = ConcurrentDiskUtil.getFileContent(switchFile.getPath(),
                            Charset.defaultCharset().toString());
                    if (!StringUtils.isEmpty(failover)) {
                        String[] lines = failover.split(DiskCache.getLineSeparator());
                        
                        for (String line : lines) {
                            String line1 = line.trim();
                            // 1表示开启故障转移模式
                            if (IS_FAILOVER_MODE.equals(line1)) {
                                switchParams.put(FAILOVER_MODE_PARAM, Boolean.TRUE.toString());
                                NAMING_LOGGER.info("failover-mode is on");
                                // 执行线程FailoverFileReader
                                new FailoverFileReader().run();
                            } else if (NO_FAILOVER_MODE.equals(line1)) {
                                // 0表示关闭故障转移模式
                                switchParams.put(FAILOVER_MODE_PARAM, Boolean.FALSE.toString());
                                NAMING_LOGGER.info("failover-mode is off");
                            }
                        }
                    } else {
                        switchParams.put(FAILOVER_MODE_PARAM, Boolean.FALSE.toString());
                    }
                }
                
            } catch (Throwable e) {
                NAMING_LOGGER.error("[NA] failed to read failover switch.", e);
            }
        }
    }
    
    class FailoverFileReader implements Runnable {
        
        @Override
        public void run() {
            Map<String, ServiceInfo> domMap = new HashMap<>(16);
            
            BufferedReader reader = null;
            try {
                // 读取failover目录存储ServiceInfo的文件内容，然后转换成ServiceInfo，
                // 并用将所有的ServiceInfo存储在FailoverReactor的serviceMap属性中

                // 读取failover目录下的所有文件，进行遍历处理
                File cacheDir = new File(failoverDir);
                // 如果文件不存在，跳过
                if (!cacheDir.exists() && !cacheDir.mkdirs()) {
                    throw new IllegalStateException("failed to create cache dir: " + failoverDir);
                }
                
                File[] files = cacheDir.listFiles();
                if (files == null) {
                    return;
                }
                
                for (File file : files) {
                    if (!file.isFile()) {
                        continue;
                    }

                    // 如果是故障转移标志文件，则跳过
                    if (file.getName().equals(UtilAndComs.FAILOVER_SWITCH)) {
                        continue;
                    }
                    
                    ServiceInfo dom = null;
                    
                    try {
                        dom = new ServiceInfo(URLDecoder.decode(file.getName(), StandardCharsets.UTF_8.name()));
                        String dataString = ConcurrentDiskUtil.getFileContent(file,
                                Charset.defaultCharset().toString());
                        reader = new BufferedReader(new StringReader(dataString));
                        
                        String json;
                        if ((json = reader.readLine()) != null) {
                            try {
                                // 读取文件中的json内容，转化为ServiceInfo对象
                                dom = JacksonUtils.toObj(json, ServiceInfo.class);
                            } catch (Exception e) {
                                NAMING_LOGGER.error("[NA] error while parsing cached dom : {}", json, e);
                            }
                        }
                        
                    } catch (Exception e) {
                        NAMING_LOGGER.error("[NA] failed to read cache for dom: {}", file.getName(), e);
                    } finally {
                        try {
                            if (reader != null) {
                                reader.close();
                            }
                        } catch (Exception e) {
                            //ignore
                        }
                    }
                    // 将ServiceInfo对象放入domMap当中
                    if (dom != null && !CollectionUtils.isEmpty(dom.getHosts())) {
                        domMap.put(dom.getKey(), dom);
                    }
                }
            } catch (Exception e) {
                NAMING_LOGGER.error("[NA] failed to read cache file", e);
            }

            // 如果domMap不为空，则将其赋值给serviceMap
            if (domMap.size() > 0) {
                serviceMap = domMap;
            }
        }
    }
    
    class DiskFileWriter extends TimerTask {
        
        @Override
        public void run() {
            // 获取ServiceInfoHolder中缓存的ServiceInfo，判断是否满足写入磁盘文件，如果满足，
            // 则将其写入前面拼接的故障转移目录：${user.home}/nacos/naming/public/failover
            Map<String, ServiceInfo> map = serviceInfoHolder.getServiceInfoMap();
            for (Map.Entry<String, ServiceInfo> entry : map.entrySet()) {
                ServiceInfo serviceInfo = entry.getValue();
                if (StringUtils.equals(serviceInfo.getKey(), UtilAndComs.ALL_IPS) || StringUtils
                        .equals(serviceInfo.getName(), UtilAndComs.ENV_LIST_KEY) || StringUtils
                        .equals(serviceInfo.getName(), UtilAndComs.ENV_CONFIGS) || StringUtils
                        .equals(serviceInfo.getName(), UtilAndComs.VIP_CLIENT_FILE) || StringUtils
                        .equals(serviceInfo.getName(), UtilAndComs.ALL_HOSTS)) {
                    continue;
                }
                // 将缓存内容写入磁盘文件
                DiskCache.write(serviceInfo, failoverDir);
            }
        }
    }
    
    public boolean isFailoverSwitch() {
        return Boolean.parseBoolean(switchParams.get(FAILOVER_MODE_PARAM));
    }
    
    public ServiceInfo getService(String key) {
        ServiceInfo serviceInfo = serviceMap.get(key);
        
        if (serviceInfo == null) {
            serviceInfo = new ServiceInfo();
            serviceInfo.setName(key);
        }
        
        return serviceInfo;
    }
}
