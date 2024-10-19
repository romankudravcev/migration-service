package de.migrationService.services;

import de.migrationService.models.Cluster;
import de.migrationService.services.kubernetesResources.ConfigMapService;
import de.migrationService.services.kubernetesResources.NamespaceService;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CustomObjectsApi;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1ServiceList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Objects;

@Service
public class RedirectService {

    private static final Logger logger = LoggerFactory.getLogger(RedirectService.class);

    // Services
    private final YamlService yamlService;
    private final NamespaceService namespaceService;
    private final ConfigMapService configMapService;

    // HTTP proxy variables
    @Value("${reverse-proxy-http.yaml-url}")
    private String httpProxyYamlUrl;

    @Value("${reverse-proxy-http.port}")
    private int httpProxyPort;

    @Autowired
    public RedirectService(YamlService yamlService,
                           NamespaceService namespaceService,
                           ConfigMapService configMapService) {
        this.yamlService = yamlService;
        this.namespaceService = namespaceService;
        this.configMapService = configMapService;
    }

    /**
     * Reroutes ingress from one cluster to another.
     *
     * @param cluster1 The source cluster.
     * @param cluster2 The target cluster.
     * @throws ApiException If an error occurs while interacting with the Kubernetes API.
     */
    public void rerouteIngress(Cluster cluster1, Cluster cluster2) throws ApiException {
        // Get the LoadBalancer IP of the second cluster
        String lbIpCluster2 = getLoadBalancerIP(cluster2.serviceList);

        // Create HTTP proxy resources in the first cluster
        createHttpProxyResources(cluster1, lbIpCluster2);

        // Delete all ingress routes except the one for the proxy
        deleteIngressRoutesExceptProxy(cluster1);
    }

    /**
     * Creates HTTP proxy resources in the specified cluster.
     *
     * @param cluster The cluster where resources will be created.
     * @param lbIp    The LoadBalancer IP address.
     */
    public void createHttpProxyResources(Cluster cluster, String lbIp) {
        namespaceService.createNamespace(cluster, "proxy");
        Map<String, String> data = Map.of(
                "TARGET_URL", lbIp,
                "PORT", String.valueOf(httpProxyPort)
        );
        configMapService.createConfigMap(cluster, "http-proxy-config", "proxy", data);
        yamlService.createResourcesFromYaml(cluster, httpProxyYamlUrl, httpProxyPort);
    }

    /**
     * Retrieves the LoadBalancer IP from the service list.
     *
     * @param serviceList The list of services.
     * @return The LoadBalancer IP address.
     */
    private String getLoadBalancerIP(V1ServiceList serviceList) {
        for (V1Service service : serviceList.getItems()) {
            if (service.getStatus() == null || service.getStatus().getLoadBalancer() == null || service.getStatus().getLoadBalancer().getIngress() == null) {
                continue;
            }
            String ip = service.getStatus().getLoadBalancer().getIngress().get(0).getIp();
            logger.info("Service: {} IP: {}", Objects.requireNonNull(service.getMetadata()).getName(), ip);
            return ip;
        }
        throw new RuntimeException("No LoadBalancer IP found");
    }

    /**
     * Deletes all ingress routes in the cluster except the one for the proxy.
     *
     * @param cluster The cluster from which ingress routes will be deleted.
     * @throws ApiException If an error occurs while interacting with the Kubernetes API.
     */
    private void deleteIngressRoutesExceptProxy(Cluster cluster) throws ApiException {
        ApiClient client = cluster.appsV1Api.getApiClient();
        CustomObjectsApi customObjectsApi = new CustomObjectsApi(client);

        for (Map<String, Object> ingressRoute : cluster.ingressRouteList) {
            @SuppressWarnings("unchecked")
            Map<String, Object> metadata = (Map<String, Object>) ingressRoute.get("metadata");

            String namespace = (String) metadata.get("namespace");
            String name = (String) metadata.get("name");

            if ("proxy".equals(namespace)) {
                continue;
            }

            logger.info("Deleting IngressRoute: {} in namespace: {}", name, namespace);

            customObjectsApi.deleteNamespacedCustomObject(
                    "traefik.containo.us",
                    "v1alpha1",
                    namespace,
                    "ingressroutes",
                    name,
                    null,
                    null,
                    null,
                    null,
                    null
            );
        }
    }
}