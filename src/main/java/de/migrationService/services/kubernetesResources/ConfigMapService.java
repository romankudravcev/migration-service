package de.migrationService.services.kubernetesResources;

import de.migrationService.models.Cluster;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1ConfigMapList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Objects;

@Service
public class ConfigMapService {

    private static final Logger logger = LoggerFactory.getLogger(ConfigMapService.class);

    /**
     * Gets the difference in config maps between two clusters.
     *
     * @param cluster1 The first cluster.
     * @param cluster2 The second cluster.
     * @return A list of config maps that are in the first cluster but not in the second.
     */
    public V1ConfigMapList getDiffConfigMaps(Cluster cluster1, Cluster cluster2) {
        V1ConfigMapList diffConfigMaps = new V1ConfigMapList();

        cluster1.configMapList.getItems().stream()
                .filter(configMap -> cluster2.configMapList.getItems().stream()
                        .noneMatch(configMap2 -> Objects.equals(Objects.requireNonNull(configMap2.getMetadata()).getName(), Objects.requireNonNull(configMap.getMetadata()).getName())))
                .forEach(configMap -> diffConfigMaps.getItems().add(configMap));

        logger.info("ConfigMap diff: ");
        diffConfigMaps.getItems().forEach(configMap -> logger.info(Objects.requireNonNull(configMap.getMetadata()).getName()));

        return diffConfigMaps;
    }

    /**
     * Creates config maps in the specified cluster.
     *
     * @param cluster    The cluster where config maps will be created.
     * @param configMaps The list of config maps to be created.
     */
    public void createConfigMaps(Cluster cluster, V1ConfigMapList configMaps) {
        configMaps.getItems().forEach(configMap -> {
            try {
                cluster.coreV1Api.createNamespacedConfigMap(
                        Objects.requireNonNull(configMap.getMetadata()).getNamespace(),
                        cleanConfigMapForCreation(configMap),
                        null,
                        null,
                        null,
                        null
                );
                logger.info("ConfigMap from diff created: {}", Objects.requireNonNull(configMap.getMetadata()).getName());
            } catch (Exception e) {
                logger.error("Error creating ConfigMap: {}", Objects.requireNonNull(configMap.getMetadata()).getName(), e);
            }
        });
    }

    /**
     * Creates a single config map in the specified cluster.
     *
     * @param cluster   The cluster where the config map will be created.
     * @param name      The name of the config map.
     * @param namespace The namespace of the config map.
     * @param data      The data of the config map.
     */
    public void createConfigMap(Cluster cluster, String name, String namespace, Map<String, String> data) {
        try {
            // Check if the config map already exists
            V1ConfigMap existingConfigMap = cluster.coreV1Api.readNamespacedConfigMap(name, namespace, null);
            if (existingConfigMap != null) {
                logger.info("ConfigMap '{}' already exists in namespace '{}'. Skipping creation.", name, namespace);
                return;
            }
        } catch (ApiException e) {
            // A 404 error indicates that the config map doesn't exist, so we can proceed with creation.
            if (e.getCode() != 404) {
                logger.error("Error checking for existing config map: {}", e.getResponseBody(), e);
                throw new RuntimeException(e);
            }
        }

        // Create the config map if it doesn't exist
        V1ConfigMap configMap = new V1ConfigMap()
                .metadata(new io.kubernetes.client.openapi.models.V1ObjectMeta().name(name).namespace(namespace))
                .data(data);

        try {
            cluster.coreV1Api.createNamespacedConfigMap(namespace, configMap, null, null, null, null);
            logger.info("ConfigMap created: {}", name);
        } catch (Exception e) {
            logger.error("Error creating ConfigMap: {}", name, e);
        }
    }

    /**
     * Cleans a config map for creation by removing unnecessary fields.
     *
     * @param originalConfigMap The original config map.
     * @return The cleaned config map.
     */
    private V1ConfigMap cleanConfigMapForCreation(V1ConfigMap originalConfigMap) {
        return new V1ConfigMap()
                .apiVersion("v1")
                .kind("ConfigMap")
                .metadata(originalConfigMap.getMetadata())
                .data(originalConfigMap.getData());
    }
}