package de.migrationService.services;

import de.migrationService.models.Cluster;
import io.kubernetes.client.openapi.apis.CustomObjectsApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class KubernetesResourceService {

    private static final Logger logger = LoggerFactory.getLogger(KubernetesResourceService.class);

    /**
     * Gets the difference in resources between two clusters.
     *
     * @param cluster1 The first cluster.
     * @param cluster2 The second cluster.
     * @param group    The API group of the resource.
     * @param version  The API version of the resource.
     * @param plural   The plural name of the resource.
     * @return A list of resources that are in the first cluster but not in the second.
     */
    public List<Map<String, Object>> getDiffResources(Cluster cluster1, Cluster cluster2, String group, String version, String plural) {
        CustomObjectsApi customObjectsApi1 = new CustomObjectsApi(cluster1.appsV1Api.getApiClient());
        CustomObjectsApi customObjectsApi2 = new CustomObjectsApi(cluster2.appsV1Api.getApiClient());

        try {
            Object resources1 = customObjectsApi1.listClusterCustomObject(group, version, plural, null, null, null, null, null, null, null, null, null, null);
            Object resources2 = customObjectsApi2.listClusterCustomObject(group, version, plural, null, null, null, null, null, null, null, null, null, null);

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> resource1List = (List<Map<String, Object>>) ((Map<String, Object>) resources1).get("items");
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> resource2List = (List<Map<String, Object>>) ((Map<String, Object>) resources2).get("items");

            Set<String> resource2Keys = resource2List.stream()
                    .map(resource -> {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> metadata = (Map<String, Object>) resource.get("metadata");
                        return metadata.get("namespace") + ":" + metadata.get("name");
                    })
                    .collect(Collectors.toSet());

            List<Map<String, Object>> missingResources = resource1List.stream()
                    .filter(resource -> {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> metadata = (Map<String, Object>) resource.get("metadata");
                        String key = metadata.get("namespace") + ":" + metadata.get("name");
                        return !resource2Keys.contains(key);
                    })
                    .collect(Collectors.toList());

            logger.info("Missing Resources:");
            missingResources.forEach(resource -> {
                @SuppressWarnings("unchecked")
                Map<String, Object> metadata = (Map<String, Object>) resource.get("metadata");
                logger.info("{}:{}", metadata.get("namespace"), metadata.get("name"));
            });

            return missingResources;

        } catch (Exception e) {
            logger.error("Error getting diff resources", e);
        }

        return List.of();
    }

    /**
     * Creates resources in the specified cluster.
     *
     * @param cluster   The cluster where resources will be created.
     * @param resources The list of resources to be created.
     * @param group     The API group of the resource.
     * @param version   The API version of the resource.
     * @param plural    The plural name of the resource.
     */
    public void createResources(Cluster cluster, List<Map<String, Object>> resources, String group, String version, String plural) {
        CustomObjectsApi customObjectsApi = new CustomObjectsApi(cluster.appsV1Api.getApiClient());

        resources.forEach(resource -> {
            try {
                @SuppressWarnings("unchecked")
                Map<String, Object> metadata = (Map<String, Object>) resource.get("metadata");
                String namespace = (String) metadata.get("namespace");
                String name = (String) metadata.get("name");

                cleanResourceForCreation(resource);

                customObjectsApi.createNamespacedCustomObject(group, version, namespace, plural, resource, null, null, null);

                logger.info("Resource created in target cluster: {}:{}", namespace, name);
            } catch (Exception e) {
                @SuppressWarnings("unchecked")
                Object namespace = ((Map<String, Object>) resource.get("metadata")).get("namespace");
                @SuppressWarnings("unchecked")
                Object name = ((Map<String, Object>) resource.get("metadata")).get("name");
                logger.error("Error creating Resource in target cluster: {}:{}", namespace, name, e);
            }
        });
    }

    /**
     * Cleans a resource for creation by removing unnecessary fields.
     *
     * @param resource The original resource.
     */
    private void cleanResourceForCreation(Map<String, Object> resource) {
        @SuppressWarnings("unchecked")
        Map<String, Object> metadata = (Map<String, Object>) resource.get("metadata");
        metadata.remove("resourceVersion");
        metadata.remove("uid");
    }
}