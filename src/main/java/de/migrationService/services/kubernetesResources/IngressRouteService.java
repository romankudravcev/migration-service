package de.migrationService.services.kubernetesResources;

import de.migrationService.models.Cluster;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CustomObjectsApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class IngressRouteService {

    private static final Logger logger = LoggerFactory.getLogger(IngressRouteService.class);

    /**
     * Gets the difference in ingress routes between two clusters.
     *
     * @param cluster1 The first cluster.
     * @param cluster2 The second cluster.
     * @return A list of ingress routes that are in the first cluster but not in the second.
     */
    public List<Map<String, Object>> getDiffIngressRoutes(Cluster cluster1, Cluster cluster2) {
        CustomObjectsApi customObjectsApi1 = new CustomObjectsApi(cluster1.appsV1Api.getApiClient());
        CustomObjectsApi customObjectsApi2 = new CustomObjectsApi(cluster2.appsV1Api.getApiClient());

        String group = "traefik.containo.us";
        String version = "v1alpha1";
        String plural = "ingressroutes";

        try {
            Object ingressRoutes1 = customObjectsApi1.listClusterCustomObject(
                    group, version, plural, null, null, null, null, null, null, null, null, null, null);
            Object ingressRoutes2 = customObjectsApi2.listClusterCustomObject(
                    group, version, plural, null, null, null, null, null, null, null, null, null, null);

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> ingressRoute1List = (List<Map<String, Object>>) ((Map<String, Object>) ingressRoutes1).get("items");
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> ingressRoute2List = (List<Map<String, Object>>) ((Map<String, Object>) ingressRoutes2).get("items");

            Set<String> ingressRoute2Keys = ingressRoute2List.stream()
                    .map(ir -> {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> metadata = (Map<String, Object>) ir.get("metadata");
                        return metadata.get("namespace") + ":" + metadata.get("name");
                    })
                    .collect(Collectors.toSet());

            List<Map<String, Object>> missingIngressRoutes = ingressRoute1List.stream()
                    .filter(ir -> {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> metadata = (Map<String, Object>) ir.get("metadata");
                        String key = metadata.get("namespace") + ":" + metadata.get("name");
                        return !ingressRoute2Keys.contains(key);
                    })
                    .collect(Collectors.toList());

            logger.info("Missing IngressRoutes:");
            missingIngressRoutes.forEach(ir -> {
                @SuppressWarnings("unchecked")
                Map<String, Object> metadata = (Map<String, Object>) ir.get("metadata");
                logger.info("{}:{}", metadata.get("namespace"), metadata.get("name"));
            });

            return missingIngressRoutes;

        } catch (Exception e) {
            logger.error("Error getting diff ingress routes", e);
        }

        return new ArrayList<>();
    }

    /**
     * Creates ingress routes in the specified cluster.
     *
     * @param cluster       The cluster where ingress routes will be created.
     * @param ingressRoutes The list of ingress routes to be created.
     */
    public void createIngressRoutes(Cluster cluster, List<Map<String, Object>> ingressRoutes) {
        CustomObjectsApi customObjectsApi = new CustomObjectsApi(cluster.appsV1Api.getApiClient());
        String group = "traefik.containo.us";
        String version = "v1alpha1";
        String plural = "ingressroutes";

        ingressRoutes.forEach(ingressRoute -> {
            try {
                @SuppressWarnings("unchecked")
                Map<String, Object> metadata = (Map<String, Object>) ingressRoute.get("metadata");
                String namespace = (String) metadata.get("namespace");
                String name = (String) metadata.get("name");

                cleanIngressRouteForCreation(ingressRoute);

                customObjectsApi.createNamespacedCustomObject(
                        group, version, namespace, plural, ingressRoute, null, null, null);

                logger.info("IngressRoute created in target cluster: {}:{}", namespace, name);
            } catch (Exception e) {
                @SuppressWarnings("unchecked")
                Object namespace = ((Map<String, Object>) ingressRoute.get("metadata")).get("namespace");
                @SuppressWarnings("unchecked")
                Object name = ((Map<String, Object>) ingressRoute.get("metadata")).get("name");
                logger.error("Error creating IngressRoute in target cluster: {}:{}", namespace, name, e);
            }
        });
    }

    /**
     * Creates a single ingress route in the specified cluster.
     *
     * @param cluster     The cluster where the ingress route will be created.
     * @param name        The name of the ingress route.
     * @param namespace   The namespace of the ingress route.
     * @param serviceName The name of the service.
     * @param servicePort The port of the service.
     * @param match       The match rule for the ingress route.
     */
    public void createIngressRoute(Cluster cluster, String name, String namespace, String serviceName, int servicePort, String match) {
        String group = "traefik.containo.us";
        String version = "v1alpha1";
        String plural = "ingressroutes";

        try {
            // Check if the IngressRoute already exists.
            Object existingIngressRoute = cluster.customObjectsApi.getNamespacedCustomObject(
                    group, version, namespace, plural, name);
            if (existingIngressRoute != null) {
                logger.info("IngressRoute with name {} already exists in namespace {}. Skipping creation.", name, namespace);
                return;
            }
        } catch (ApiException e) {
            // A 404 error indicates that the IngressRoute doesn't exist, so we can proceed with creation.
            if (e.getCode() != 404) {
                logger.error("Error checking for existing IngressRoute: {}", e.getResponseBody());
                throw new RuntimeException(e);
            }
        }

        // Create the new IngressRoute.
        Map<String, Object> ingressRoute = new HashMap<>();
        ingressRoute.put("apiVersion", "traefik.containo.us/v1alpha1");
        ingressRoute.put("kind", "IngressRoute");
        ingressRoute.put("metadata", Map.of(
                "name", name,
                "namespace", namespace
        ));
        ingressRoute.put("spec", Map.of(
                "entryPoints", Collections.singletonList("web"),
                "routes", Collections.singletonList(Map.of(
                        "match", match,
                        "kind", "Rule",
                        "services", Collections.singletonList(Map.of(
                                "name", serviceName,
                                "port", servicePort
                        ))
                ))
        ));

        try {
            cluster.customObjectsApi.createNamespacedCustomObject(
                    group, version, namespace, plural, ingressRoute, null, null, null);
            logger.info("IngressRoute {} created successfully in namespace {}.", name, namespace);
        } catch (ApiException e) {
            logger.error("Error creating IngressRoute: {}", e.getResponseBody());
            throw new RuntimeException(e);
        }
    }

    /**
     * Cleans an ingress route for creation by removing unnecessary fields.
     *
     * @param ingressRoute The original ingress route.
     */
    private void cleanIngressRouteForCreation(Map<String, Object> ingressRoute) {
        @SuppressWarnings("unchecked")
        Map<String, Object> metadata = (Map<String, Object>) ingressRoute.get("metadata");
        metadata.remove("resourceVersion");
        metadata.remove("uid");
    }

    /**
     * Creates an ingress route from a YAML map in the specified cluster.
     *
     * @param cluster The cluster where the ingress route will be created.
     * @param yamlMap The YAML map representing the ingress route.
     * @param port    The port to set in the ingress route spec.
     */
    public void createIngressRouteFromYaml(Cluster cluster, Map<String, Object> yamlMap, int port) {
        // Set the port in the ingress route spec if needed
        @SuppressWarnings("unchecked")
        Map<String, Object> spec = (Map<String, Object>) yamlMap.get("spec");
        if (spec != null) {
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> routes = (List<Map<String, Object>>) spec.get("routes");
            if (routes != null) {
                for (Map<String, Object> route : routes) {
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> services = (List<Map<String, Object>>) route.get("services");
                    if (services != null) {
                        for (Map<String, Object> service : services) {
                            service.put("port", port);
                        }
                    }
                }
            }
        }

        try {
            // Create the ingress route in the specified namespace
            CustomObjectsApi customObjectsApi = new CustomObjectsApi(cluster.appsV1Api.getApiClient());

            @SuppressWarnings("unchecked")
            String namespace = Objects.requireNonNull((String) ((Map<String, Object>) yamlMap.get("metadata")).get("namespace"));
            @SuppressWarnings("unchecked")
            String name = Objects.requireNonNull((String) ((Map<String, Object>) yamlMap.get("metadata")).get("name"));

            customObjectsApi.createNamespacedCustomObject(
                    "traefik.containo.us", "v1alpha1",
                    namespace,
                    "ingressroutes", yamlMap, null, null, null
            );
            logger.info("IngressRoute created from YAML: {}", name);
        } catch (ApiException e) {
            logger.error("Failed to create IngressRoute from YAML: {}", e.getResponseBody(), e);
        } catch (Exception e) {
            logger.error("Unexpected error occurred while creating IngressRoute from YAML", e);
        }
    }
}