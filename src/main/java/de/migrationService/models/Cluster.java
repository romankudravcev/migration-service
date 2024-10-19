package de.migrationService.models;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.apis.CustomObjectsApi;
import io.kubernetes.client.openapi.apis.NetworkingV1Api;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1ConfigMapList;
import io.kubernetes.client.openapi.models.V1Deployment;
import io.kubernetes.client.openapi.models.V1DeploymentList;
import io.kubernetes.client.openapi.models.V1IngressList;
import io.kubernetes.client.openapi.models.V1Namespace;
import io.kubernetes.client.openapi.models.V1NamespaceList;
import io.kubernetes.client.openapi.models.V1Secret;
import io.kubernetes.client.openapi.models.V1SecretList;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1ServiceList;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.KubeConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class Cluster {
    private static final Logger logger = LoggerFactory.getLogger(Cluster.class);

    // Kubernetes API
    public final AppsV1Api appsV1Api;
    public final CoreV1Api coreV1Api;
    public final NetworkingV1Api networkingV1Api;
    public final CustomObjectsApi customObjectsApi;

    public V1NamespaceList namespaceList = new V1NamespaceList();
    public V1DeploymentList deploymentList = new V1DeploymentList();
    public V1ServiceList serviceList = new V1ServiceList();
    public V1IngressList ingressList = new V1IngressList();
    public List<Map<String, Object>> ingressRouteList = new ArrayList<>();
    public V1ConfigMapList configMapList = new V1ConfigMapList();
    public V1SecretList secretList = new V1SecretList();

    public Cluster(MultipartFile kubeConfigFile) {
        ApiClient client = loadKubeConfig(kubeConfigFile);
        this.appsV1Api = new AppsV1Api(client);
        this.coreV1Api = new CoreV1Api(client);
        this.networkingV1Api = new NetworkingV1Api(client);
        this.customObjectsApi = new CustomObjectsApi(client);

        loadCluster();
    }

    public void loadCluster() {
        getNamespaces();
        getDeployments();
        getServices();
        getIngress();
        getIngressRoutes();
        getConfigMaps();
        getSecrets();
    }

    private void getNamespaces() {
        try {
            namespaceList = coreV1Api.listNamespace(null, null, null, null, null, null, null, null, null, false);
            logger.info("Namespaces loaded successfully");
        } catch (ApiException e) {
            logger.error("Error getting namespaces", e);
            throw new RuntimeException(e);
        }
    }

    private void getDeployments() {
        try {
            deploymentList = appsV1Api.listDeploymentForAllNamespaces(null, null, null, null, null, null, null, null, null, null);
            logger.info("Deployments loaded successfully");
        } catch (ApiException e) {
            logger.error("Error getting deployments", e);
            throw new RuntimeException(e);
        }
    }

    private void getServices() {
        try {
            serviceList = coreV1Api.listServiceForAllNamespaces(null, null, null, null, null, null, null, null, null, null);
            logger.info("Services loaded successfully");
        } catch (ApiException e) {
            logger.error("Error getting services", e);
            throw new RuntimeException(e);
        }
    }

    private void getIngress() {
        try {
            ingressList = networkingV1Api.listIngressForAllNamespaces(null, null, null, null, null, null, null, null, null, null);
            logger.info("Ingress loaded successfully");
        } catch (ApiException e) {
            logger.error("Error getting ingress", e);
            throw new RuntimeException(e);
        }
    }

    private void getIngressRoutes() {
        String group = "traefik.containo.us";
        String version = "v1alpha1";
        String plural = "ingressroutes";

        try {
            Object ingressRoutes = customObjectsApi.listClusterCustomObject(
                    group, version, plural, null, null, null, null, null, null, null, null, null, null);

            ingressRouteList = (List<Map<String, Object>>) ((Map<String, Object>) ingressRoutes).get("items");
            logger.info("IngressRoutes loaded successfully, count: {}", ingressRouteList.size());
        } catch (ApiException e) {
            logger.error("Error getting IngressRoutes", e);
            throw new RuntimeException(e);
        }
    }

    private void getConfigMaps() {
        try {
            configMapList = coreV1Api.listConfigMapForAllNamespaces(null, null, null, null, null, null, null, null, null, null);
            logger.info("Config maps loaded successfully");
        } catch (ApiException e) {
            logger.error("Error getting config maps", e);
            throw new RuntimeException(e);
        }
    }

    private void getSecrets() {
        try {
            secretList = coreV1Api.listSecretForAllNamespaces(null, null, null, null, null, null, null, null, null, null);
            logger.info("Secrets loaded successfully");
        } catch (ApiException e) {
            logger.error("Error getting secrets", e);
            throw new RuntimeException(e);
        }
    }

    public ApiClient loadKubeConfig(MultipartFile file) {
        try {
            KubeConfig kubeConfig = KubeConfig.loadKubeConfig(new InputStreamReader(file.getInputStream()));
            ApiClient client = ClientBuilder.kubeconfig(kubeConfig).build();
            logger.info("KubeConfig loaded successfully");
            return client;
        } catch (IOException e) {
            logger.error("Error loading KubeConfig", e);
            throw new RuntimeException(e);
        }
    }

    public void printCluster() {
        System.out.println("------------------");
        System.out.println("Namespaces: ");
        for (V1Namespace namespace : namespaceList.getItems()) {
            System.out.println(Objects.requireNonNull(namespace.getMetadata()).getName());
        }

        System.out.println("------------------");

        System.out.println("Deployments: ");
        for (V1Deployment deployment : deploymentList.getItems()) {
            System.out.println(Objects.requireNonNull(deployment.getMetadata()).getName());
        }

        System.out.println("------------------");

        System.out.println("Services: ");
        for (V1Service service : serviceList.getItems()) {
            System.out.println(Objects.requireNonNull(service.getMetadata()).getName());
        }

        System.out.println("------------------");

        System.out.println("ConfigMaps: ");
        for (V1ConfigMap configMap : configMapList.getItems()) {
            System.out.println(Objects.requireNonNull(configMap.getMetadata()).getName());
        }

        System.out.println("------------------");

        System.out.println("Secrets: ");
        for (V1Secret secret : secretList.getItems()) {
            System.out.println(Objects.requireNonNull(secret.getMetadata()).getName());
        }

        System.out.println("------------------");
    }
}