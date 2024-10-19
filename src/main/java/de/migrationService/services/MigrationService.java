package de.migrationService.services;

import de.migrationService.models.Cluster;
import de.migrationService.services.kubernetesResources.ConfigMapService;
import de.migrationService.services.kubernetesResources.DeploymentService;
import de.migrationService.services.kubernetesResources.IngressRouteService;
import de.migrationService.services.kubernetesResources.IngressService;
import de.migrationService.services.kubernetesResources.MiddlewareService;
import de.migrationService.services.kubernetesResources.NamespaceService;
import de.migrationService.services.kubernetesResources.SecretService;
import de.migrationService.services.kubernetesResources.ServiceService;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.models.V1ConfigMapList;
import io.kubernetes.client.openapi.models.V1DeploymentList;
import io.kubernetes.client.openapi.models.V1IngressList;
import io.kubernetes.client.openapi.models.V1NamespaceList;
import io.kubernetes.client.openapi.models.V1SecretList;
import io.kubernetes.client.openapi.models.V1ServiceList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;
import java.util.Map;

@Service
public class MigrationService {
    private static final Logger logger = LoggerFactory.getLogger(MigrationService.class);

    private final NamespaceService namespaceService;
    private final ConfigMapService configMapService;
    private final SecretService secretService;
    private final DeploymentService deploymentService;
    private final ServiceService serviceService;
    private final IngressService ingressService;
    private final MiddlewareService middlewareService;
    private final IngressRouteService ingressRouteService;
    private final RedirectService redirectService;

    @Autowired
    public MigrationService(NamespaceService namespaceService,
                            ConfigMapService configMapService,
                            SecretService secretService,
                            DeploymentService deploymentService,
                            ServiceService serviceService,
                            IngressService ingressService,
                            MiddlewareService middlewareService,
                            IngressRouteService ingressRouteService,
                            RedirectService redirectService) {
        this.namespaceService = namespaceService;
        this.configMapService = configMapService;
        this.secretService = secretService;
        this.deploymentService = deploymentService;
        this.serviceService = serviceService;
        this.ingressService = ingressService;
        this.middlewareService = middlewareService;
        this.ingressRouteService = ingressRouteService;
        this.redirectService = redirectService;
    }

    /**
     * Migrates resources from one cluster to another.
     *
     * @param file1 The file representing the first cluster.
     * @param file2 The file representing the second cluster.
     * @throws ApiException If an error occurs while interacting with the Kubernetes API.
     */
    public void migrate(MultipartFile file1, MultipartFile file2) throws ApiException {
        // Load Clusters
        Cluster cluster1 = new Cluster(file1);
        Cluster cluster2 = new Cluster(file2);

        // Get diff namespaces and create them
        V1NamespaceList diffNamespaces = namespaceService.getDiffNamespaces(cluster1, cluster2);
        namespaceService.createNamespaces(cluster2, diffNamespaces);
        logger.info("All namespaces copied");

        V1ConfigMapList diffConfigMaps = configMapService.getDiffConfigMaps(cluster1, cluster2);
        configMapService.createConfigMaps(cluster2, diffConfigMaps);
        logger.info("All config maps copied");

        V1SecretList diffSecrets = secretService.getDiffSecrets(cluster1, cluster2);
        secretService.createSecrets(cluster2, diffSecrets);
        logger.info("All secrets copied");

        // Get diff deployments and create them
        V1DeploymentList diffDeployments = deploymentService.getDiffDeployments(cluster1, cluster2);
        deploymentService.createDeployments(cluster2, diffDeployments);
        logger.info("All deployments copied");

        // Get diff services and create them
        V1ServiceList diffServices = serviceService.getDiffServices(cluster1, cluster2);
        serviceService.createServices(cluster2, diffServices);
        logger.info("All services copied");

        // Get diff ingresses and create them
        V1IngressList diffIngress = ingressService.getDiffIngress(cluster1, cluster2);
        ingressService.createIngress(cluster2, diffIngress);
        logger.info("All ingresses copied");

        // Get diff middlewares and create them
        List<Map<String, Object>> diffMiddlewares = middlewareService.getDiffMiddlewares(cluster1, cluster2);
        middlewareService.createMiddleware(cluster2, diffMiddlewares);
        logger.info("All middlewares copied");

        // Get diff ingress routes and create them
        List<Map<String, Object>> diffIngressRoutes = ingressRouteService.getDiffIngressRoutes(cluster1, cluster2);
        ingressRouteService.createIngressRoutes(cluster2, diffIngressRoutes);
        logger.info("All ingress routes copied");

        // Reroute original ingresses to new cluster
        redirectService.rerouteIngress(cluster1, cluster2);
        logger.info("Reroute successful");
    }
}