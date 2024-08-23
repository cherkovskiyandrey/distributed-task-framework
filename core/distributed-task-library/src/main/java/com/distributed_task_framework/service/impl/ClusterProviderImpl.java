package com.distributed_task_framework.service.impl;

import com.distributed_task_framework.mapper.NodeStateMapper;
import com.distributed_task_framework.model.Capabilities;
import com.distributed_task_framework.model.NodeLoading;
import com.distributed_task_framework.persistence.entity.CapabilityEntity;
import com.distributed_task_framework.persistence.entity.NodeStateEntity;
import com.distributed_task_framework.persistence.repository.CapabilityRepository;
import com.distributed_task_framework.persistence.repository.NodeStateRepository;
import com.distributed_task_framework.service.internal.CapabilityRegisterProvider;
import com.distributed_task_framework.service.internal.ClusterProvider;
import com.distributed_task_framework.settings.CommonSettings;
import com.distributed_task_framework.utils.ExecutorUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.sun.management.OperatingSystemMXBean;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.experimental.NonFinal;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.ReflectionUtils;

import java.lang.management.ManagementFactory;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class ClusterProviderImpl implements ClusterProvider {
    public static final double UNDEFINED_METRIC_VALUE = -1.;

    private static final OperatingSystemMXBean OPERATING_SYSTEM_MX_BEAN =
        (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

    UUID nodeId;
    CommonSettings commonSettings;
    CapabilityRegisterProvider capabilityRegisterProvider;
    NodeStateRepository nodeStateRepository;
    CapabilityRepository capabilityRepository;
    PlatformTransactionManager transactionManager;
    CacheManager cacheManager;
    NodeStateMapper nodeStateMapper;
    ScheduledExecutorService scheduledExecutorService;
    @NonFinal
    int currentCpuLoadingMetricsPosition;
    double[] currentCpuLoadingMetrics;
    Clock clock;

    public ClusterProviderImpl(CommonSettings commonSettings,
                               CapabilityRegisterProvider capabilityRegisterProvider,
                               PlatformTransactionManager transactionManager,
                               CacheManager cacheManager,
                               NodeStateMapper nodeStateMapper,
                               NodeStateRepository nodeStateRepository,
                               CapabilityRepository capabilityRepository,
                               Clock clock) {
        this.nodeId = UUID.randomUUID();
        this.commonSettings = commonSettings;
        this.capabilityRegisterProvider = capabilityRegisterProvider;
        this.transactionManager = transactionManager;
        this.cacheManager = cacheManager;
        this.nodeStateMapper = nodeStateMapper;
        this.nodeStateRepository = nodeStateRepository;
        this.capabilityRepository = capabilityRepository;
        this.currentCpuLoadingMetricsPosition = 0;
        this.currentCpuLoadingMetrics = initCpuLoadingMetricsStore(commonSettings);
        this.clock = clock;
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
            .setDaemon(false)
            .setNameFormat("dtf-watchdog-%d")
            .setUncaughtExceptionHandler((t, e) -> {
                log.error("ClusterProviderImpl(): error when try to update", e);
                ReflectionUtils.rethrowRuntimeException(e);
            })
            .build()
        );
    }

    private double[] initCpuLoadingMetricsStore(CommonSettings commonSettings) {
        Duration cpuMetricsTimeWindow = commonSettings.getRegistrySettings().getCpuCalculatingTimeWindow();
        Integer updateFixedDelayMs = commonSettings.getRegistrySettings().getUpdateFixedDelayMs();
        int metricSize = Math.max((int) (cpuMetricsTimeWindow.toMillis() / updateFixedDelayMs), 1);
        double[] result = new double[metricSize];
        Arrays.fill(result, UNDEFINED_METRIC_VALUE);
        return result;
    }

    @PostConstruct
    public void init() {
        log.info("init(): nodeId=[{}]", nodeId);
        scheduledExecutorService.scheduleWithFixedDelay(
            ExecutorUtils.wrapRepeatableRunnable(this::watchdog),
            commonSettings.getRegistrySettings().getUpdateInitialDelayMs(),
            commonSettings.getRegistrySettings().getUpdateFixedDelayMs(),
            TimeUnit.MILLISECONDS
        );
    }

    public void watchdog() {
        TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
        transactionTemplate.executeWithoutResult(status -> {
                //it is important to update state and capabilities atomically
                //because some capabilities are known before start the node and immutable
                //and have to be visible atomically along with node registration
                updateOwnState();
                updateCapabilities();
            }
        );
        transactionTemplate.executeWithoutResult(status -> cleanObsoleteNodes());
    }

    private void updateOwnState() {
        currentCpuLoadingMeasureAndUpdate();
        Optional<NodeStateEntity> nodeStateEntityOpt = nodeStateRepository.findById(nodeId);
        NodeStateEntity nodeStateEntity = nodeStateEntityOpt
            .map(currentNodeState -> currentNodeState.toBuilder()
                .lastUpdateDateUtc(LocalDateTime.now(clock))
                .medianCpuLoading(measureCurrentMedianCpuLoading())
                .build())
            .orElse(
                NodeStateEntity.builder()
                    .node(nodeId)
                    .lastUpdateDateUtc(LocalDateTime.now(clock))
                    .medianCpuLoading(measureCurrentMedianCpuLoading())
                    .build()
            );
        nodeStateRepository.save(nodeStateEntity);
    }

    private void currentCpuLoadingMeasureAndUpdate() {
        double currentCpuLoading = Math.max(OPERATING_SYSTEM_MX_BEAN.getCpuLoad(), UNDEFINED_METRIC_VALUE);
        currentCpuLoadingMetrics[currentCpuLoadingMetricsPosition] = currentCpuLoading;
        currentCpuLoadingMetricsPosition = (currentCpuLoadingMetricsPosition + 1) % currentCpuLoadingMetrics.length;
    }

    private Double measureCurrentMedianCpuLoading() {
        double[] copyCpuLoadingMetrics = Arrays.stream(currentCpuLoadingMetrics)
            .filter(value -> Double.compare(value, UNDEFINED_METRIC_VALUE) != 0)
            .toArray();
        Arrays.sort(copyCpuLoadingMetrics);
        double value = copyCpuLoadingMetrics[copyCpuLoadingMetrics.length / 2];
        return Double.compare(value, UNDEFINED_METRIC_VALUE) != 0 ? value : null;
    }

    @VisibleForTesting
    void updateCapabilities() {
        Set<CapabilityEntity> publishedCurrentCapabilities = capabilityRepository.findByNodeId(nodeId);
        Set<CapabilityEntity> currentCapabilities = capabilityRegisterProvider.getAllCapabilityRegister().stream()
            .flatMap(capabilityRegister -> capabilityRegister.capabilities().stream()
                .filter(capability -> Capabilities.UNKNOWN != capability)
                .map(capability -> CapabilityEntity.builder()
                    .value(capability.toString())
                    .nodeId(nodeId)
                    .build()
                )
            ).collect(Collectors.toSet());
        boolean hasToBeUpdated = publishedCurrentCapabilities.size() != currentCapabilities.size() ||
            Sets.intersection(publishedCurrentCapabilities, currentCapabilities).size() != currentCapabilities.size();
        if (hasToBeUpdated) {
            log.info(
                "updateCapabilities(): capabilities changed from=[{}], to=[{}]",
                publishedCurrentCapabilities,
                currentCapabilities
            );
            capabilityRepository.deleteAllByNodeId(nodeId);
            capabilityRepository.saveOrUpdateBatch(currentCapabilities);
        }
    }

    private void cleanObsoleteNodes() {
        LocalDateTime lostBoundaryDate = LocalDateTime.now(clock)
            .minus(commonSettings.getRegistrySettings().getMaxInactivityIntervalMs(), ChronoUnit.MILLIS);
        Collection<UUID> lostNodes = nodeStateRepository.findLostNodes(lostBoundaryDate);
        if (!lostNodes.isEmpty()) {
            nodeStateRepository.deleteAllById(lostNodes);
            log.info("cleanObsoleteNodes(): lost nodes has been cleaned [{}]", lostNodes);
        }
    }

    /**
     * @noinspection ResultOfMethodCallIgnored
     */
    @PreDestroy
    public void shutdown() throws InterruptedException {
        log.info("shutdown(): shutdown started");
        scheduledExecutorService.shutdownNow();
        scheduledExecutorService.awaitTermination(1, TimeUnit.MINUTES);
        unregisterItself();
        log.info("shutdown(): shutdown completed");
    }

    private void unregisterItself() {
        TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
        transactionTemplate.executeWithoutResult(status -> {
            nodeStateRepository.deleteById(nodeId);
        });
    }

    @Override
    public UUID nodeId() {
        return nodeId;
    }

    @Override
    public boolean isNodeRegistered() {
        return nodeStateRepository.findById(nodeId).isPresent();
    }

    @Override
    public List<NodeLoading> currentNodeLoading() {
        return nodesWithCapabilities().keySet().stream()
            .map(nodeStateMapper::fromEntity)
            .toList();
    }

    @Override
    public Set<UUID> clusterNodes() {
        return nodesWithCapabilities().keySet().stream()
            .map(NodeStateEntity::getNode)
            .collect(Collectors.toSet());
    }

    @Override
    public Map<UUID, EnumSet<Capabilities>> clusterCapabilities() {
        return nodesWithCapabilities().entrySet().stream()
            .map(entry -> Pair.of(
                    entry.getKey().getNode(),
                    entry.getValue().stream()
                        .filter(Objects::nonNull)
                        .map(capabilityEntity -> Capabilities.from(capabilityEntity.getValue()))
                        .collect(Collectors.toSet())
                )
            )
            .filter(pair -> !pair.getValue().isEmpty())
            .collect(Collectors.toMap(
                    Pair::getKey,
                    pair -> EnumSet.copyOf(pair.getValue())
                )
            );
    }

    //We use one cache for node and it's capabilities in order to atomically see node state
    private Map<NodeStateEntity, List<CapabilityEntity>> nodesWithCapabilities() {
        return getOrCalculateValue(
            "clusterNodesAndCapabilities",
            nodeStateRepository::getAllWithCapabilities
        );
    }

    @Override
    public boolean doAllNodesSupport(Capabilities... capabilities) {
        EnumSet<Capabilities> searchedCapabilities = Capabilities.createEmpty();
        searchedCapabilities.addAll(Arrays.asList(capabilities));
        Map<UUID, EnumSet<Capabilities>> clusterCapabilities = clusterCapabilities();
        return clusterCapabilities.entrySet().stream()
            .allMatch(entry -> entry.getValue().containsAll(searchedCapabilities));
    }

    private Cache getCommonCache() {
        return Objects.requireNonNull(cacheManager.getCache("commonRegistryCacheManager"));
    }

    @SuppressWarnings("unchecked")
    private <T> T getOrCalculateValue(String key, Callable<T> calculator) {
        var wrapper = getCommonCache().get(key);
        if (wrapper != null && wrapper.get() != null) {
            return (T) wrapper.get();
        }

        //in order to avoid deadlock between acquiring transaction and calculating of new value
        return new TransactionTemplate(transactionManager).execute(status ->
            getCommonCache().get(
                key,
                calculator
            )
        );
    }
}