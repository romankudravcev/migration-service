package de.migrationService.services.kubernetesResources;

import de.migrationService.models.Cluster;
import io.kubernetes.client.openapi.apis.CustomObjectsApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class MiddlewareService {

    private static final Logger logger = LoggerFactory.getLogger(MiddlewareService.class);

    /**
     * Gets the difference in middlewares between two clusters.
     *
     * @param cluster1 The first cluster.
     * @param cluster2 The second cluster.
     * @return A list of middlewares that are in the first cluster but not in the second.
     */
    public List<Map<String, Object>> getDiffMiddlewares(Cluster cluster1, Cluster cluster2) {
        CustomObjectsApi customObjectsApi1 = new CustomObjectsApi(cluster1.appsV1Api.getApiClient());
        CustomObjectsApi customObjectsApi2 = new CustomObjectsApi(cluster2.appsV1Api.getApiClient());

        String group = "traefik.containo.us";
        String version = "v1alpha1";
        String plural = "middlewares";

        try {
            Object middlewares1 = customObjectsApi1.listClusterCustomObject(
                    group, version, plural, null, null, null, null, null, null, null, null, null, null);
            Object middlewares2 = customObjectsApi2.listClusterCustomObject(
                    group, version, plural, null, null, null, null, null, null, null, null, null, null);

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> middleware1List = (List<Map<String, Object>>) ((Map<String, Object>) middlewares1).get("items");
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> middleware2List = (List<Map<String, Object>>) ((Map<String, Object>) middlewares2).get("items");

            Set<String> middleware2Keys = middleware2List.stream()
                    .map(mw -> {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> metadata = (Map<String, Object>) mw.get("metadata");
                        return metadata.get("namespace") + ":" + metadata.get("name");
                    })
                    .collect(Collectors.toSet());

            List<Map<String, Object>> missingMiddlewares = middleware1List.stream()
                    .filter(mw -> {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> metadata = (Map<String, Object>) mw.get("metadata");
                        String key = metadata.get("namespace") + ":" + metadata.get("name");
                        return !middleware2Keys.contains(key);
                    })
                    .collect(Collectors.toList());

            logger.info("Missing Middleware:");
            missingMiddlewares.forEach(mw -> {
                @SuppressWarnings("unchecked")
                Map<String, Object> metadata = (Map<String, Object>) mw.get("metadata");
                logger.info("{}:{}", metadata.get("namespace"), metadata.get("name"));
            });

            return missingMiddlewares;

        } catch (Exception e) {
            logger.error("Error getting diff middlewares", e);
        }

        return new ArrayList<>();
    }

    /**
     * Creates middlewares in the specified cluster.
     *
     * @param cluster     The cluster where middlewares will be created.
     * @param middlewares The list of middlewares to be created.
     */
    public void createMiddleware(Cluster cluster, List<Map<String, Object>> middlewares) {
        CustomObjectsApi customObjectsApi = new CustomObjectsApi(cluster.appsV1Api.getApiClient());
        String group = "traefik.containo.us";
        String version = "v1alpha1";
        String plural = "middlewares";

        middlewares.forEach(middleware -> {
            try {
                @SuppressWarnings("unchecked")
                Map<String, Object> metadata = (Map<String, Object>) middleware.get("metadata");
                String namespace = (String) metadata.get("namespace");
                String name = (String) metadata.get("name");

                cleanMiddlewareForCreation(middleware);

                customObjectsApi.createNamespacedCustomObject(
                        group, version, namespace, plural, middleware, null, null, null);

                logger.info("Middleware created in target cluster: {}:{}", namespace, name);
            } catch (Exception e) {
                @SuppressWarnings("unchecked")
                Object namespace = ((Map<String, Object>) middleware.get("metadata")).get("namespace");
                @SuppressWarnings("unchecked")
                Object name = ((Map<String, Object>) middleware.get("metadata")).get("name");
                logger.error("Error creating Middleware in target cluster: {}:{}", namespace, name, e);


            }
        });
    }

    /**
     * Cleans a middleware for creation by removing unnecessary fields.
     *
     * @param middleware The original middleware.
     */
    private void cleanMiddlewareForCreation(Map<String, Object> middleware) {
        @SuppressWarnings("unchecked")
        Map<String, Object> metadata = (Map<String, Object>) middleware.get("metadata");
        metadata.remove("resourceVersion");
        metadata.remove("uid");
    }
}