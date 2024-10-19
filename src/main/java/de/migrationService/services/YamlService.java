package de.migrationService.services;

import de.migrationService.models.Cluster;
import de.migrationService.services.kubernetesResources.DeploymentService;
import de.migrationService.services.kubernetesResources.IngressRouteService;
import de.migrationService.services.kubernetesResources.ServiceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

@Service
public class YamlService {

    private static final Logger logger = LoggerFactory.getLogger(YamlService.class);

    private final DeploymentService deploymentService;
    private final ServiceService serviceService;
    private final IngressRouteService ingressRouteService;

    @Autowired
    public YamlService(DeploymentService deploymentService,
                       ServiceService serviceService,
                       IngressRouteService ingressRouteService) {
        this.deploymentService = deploymentService;
        this.serviceService = serviceService;
        this.ingressRouteService = ingressRouteService;
    }

    /**
     * Creates resources in the cluster from a YAML file.
     *
     * @param cluster   The cluster where resources will be created.
     * @param githubUrl The URL of the YAML file.
     * @param port      The port to be set in the resources.
     */
    public void createResourcesFromYaml(Cluster cluster, String githubUrl, int port) {
        Iterable<Object> yamlObjects = downloadYaml(githubUrl);

        // Create resources in the cluster
        for (Object yamlObject : yamlObjects) {
            if (yamlObject instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> yamlMap = (Map<String, Object>) yamlObject;
                String kind = (String) yamlMap.get("kind");

                switch (kind) {
                    case "Deployment":
                        deploymentService.createDeploymentFromYaml(cluster, yamlMap, port);
                        break;
                    case "Service":
                        serviceService.createServiceFromYaml(cluster, yamlMap, port);
                        break;
                    case "IngressRoute":
                        ingressRouteService.createIngressRouteFromYaml(cluster, yamlMap, port);
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported resource kind: " + kind);
                }
            }
        }
    }

    /**
     * Downloads and parses a YAML file from a given URL.
     *
     * @param githubUrl The URL of the YAML file.
     * @return An iterable of parsed YAML objects.
     */
    private static Iterable<Object> downloadYaml(String githubUrl) {
        try {
            URL url = new URL(githubUrl);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            InputStream inputStream = connection.getInputStream();

            // Parse YAML content
            Yaml yaml = new Yaml();
            return yaml.loadAll(inputStream);
        } catch (IOException e) {
            logger.error("Error while trying to download the YAML file from URL: {}", githubUrl, e);
            throw new RuntimeException("Error while trying to download the YAML", e);
        }
    }
}