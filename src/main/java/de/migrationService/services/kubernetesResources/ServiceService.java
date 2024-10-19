package de.migrationService.services.kubernetesResources;

import de.migrationService.models.Cluster;
import io.kubernetes.client.custom.IntOrString;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1ServiceList;
import io.kubernetes.client.openapi.models.V1ServicePort;
import io.kubernetes.client.openapi.models.V1ServiceSpec;
import io.kubernetes.client.util.Yaml;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Objects;

@Service
public class ServiceService {

    private static final Logger logger = LoggerFactory.getLogger(ServiceService.class);

    /**
     * Gets the difference in services between two clusters.
     *
     * @param cluster1 The first cluster.
     * @param cluster2 The second cluster.
     * @return A list of services that are in the first cluster but not in the second.
     */
    public V1ServiceList getDiffServices(Cluster cluster1, Cluster cluster2) {
        V1ServiceList diffServices = new V1ServiceList();
        cluster1.serviceList.getItems().stream()
                .filter(service -> cluster2.serviceList.getItems().stream()
                        .noneMatch(service2 -> Objects.equals(Objects.requireNonNull(service2.getMetadata()).getName(), Objects.requireNonNull(service.getMetadata()).getName())))
                .forEach(service -> diffServices.getItems().add(service));

        logger.info("Service diff: ");
        diffServices.getItems().forEach(service -> logger.info(Objects.requireNonNull(service.getMetadata()).getName()));
        return diffServices;
    }

    /**
     * Creates services in the specified cluster.
     *
     * @param cluster  The cluster where services will be created.
     * @param services The list of services to be created.
     */
    public void createServices(Cluster cluster, V1ServiceList services) {
        services.getItems().forEach(service -> {
            try {
                cluster.coreV1Api.createNamespacedService(
                        Objects.requireNonNull(service.getMetadata()).getNamespace(),
                        cleanServiceForCreation(service),
                        null,
                        null,
                        null,
                        null
                );
                logger.info("Service created: {}", Objects.requireNonNull(service.getMetadata()).getName());
            } catch (Exception e) {
                logger.error("Error creating service: {}", Objects.requireNonNull(service.getMetadata()).getName(), e);
            }
        });
    }

    /**
     * Creates a single service in the specified cluster.
     *
     * @param cluster        The cluster where the service will be created.
     * @param name           The name of the service.
     * @param namespace      The namespace of the service.
     * @param deploymentName The name of the deployment.
     * @param targetPort     The target port of the service.
     * @param servicePort    The service port.
     */
    public void createService(Cluster cluster, String name, String namespace, String deploymentName, int targetPort, int servicePort) {
        try {
            // Check if the service already exists
            V1Service existingService = cluster.coreV1Api.readNamespacedService(name, namespace, null);
            if (existingService != null) {
                logger.info("Service '{}' already exists in namespace '{}'. Skipping creation.", name, namespace);
                return;
            }
        } catch (ApiException e) {
            // A 404 error indicates that the service doesn't exist, so we can proceed with creation.
            if (e.getCode() != 404) {
                logger.error("Error checking for existing service: {}", e.getResponseBody(), e);
                throw new RuntimeException(e);
            }
        }

        // Create a service spec with the selector and ports
        V1ServiceSpec serviceSpec = new V1ServiceSpec()
                .selector(Map.of("app", deploymentName)) // Select pods with the same label as the deployment
                .addPortsItem(new V1ServicePort()
                        .port(servicePort)       // The port exposed by the service
                        .targetPort(new IntOrString(targetPort))); // The port on the container

        // Create the service with the specified metadata and spec
        V1Service service = new V1Service()
                .apiVersion("v1")
                .kind("Service")
                .metadata(new V1ObjectMeta().name(name).namespace(namespace))
                .spec(serviceSpec);

        try {
            // Create the service in the specified namespace
            cluster.coreV1Api.createNamespacedService(namespace, service, null, null, null, null);
            logger.info("Service '{}' created in namespace '{}' with target port {} and service port {}", name, namespace, targetPort, servicePort);
        } catch (ApiException e) {
            logger.error("Failed to create service: {}", e.getResponseBody(), e);
        }
    }

    /**
     * Cleans a service for creation by removing unnecessary fields.
     *
     * @param originalService The original service.
     * @return The cleaned service.
     */
    private V1Service cleanServiceForCreation(V1Service originalService) {
        V1ObjectMeta originalMetadata = originalService.getMetadata();
        V1ServiceSpec originalSpec = originalService.getSpec();

        assert originalMetadata != null;
        V1ObjectMeta cleanedMetadata = new V1ObjectMeta()
                .name(originalMetadata.getName())
                .namespace(originalMetadata.getNamespace())
                .labels(originalMetadata.getLabels())
                .annotations(originalMetadata.getAnnotations());

        return new V1Service()
                .apiVersion("v1")
                .kind("Service")
                .metadata(cleanedMetadata)
                .spec(originalSpec);
    }

    /**
     * Creates a service from a YAML map in the specified cluster.
     *
     * @param cluster The cluster where the service will be created.
     * @param yamlMap The YAML map representing the service.
     * @param port    The port of the service.
     */
    public void createServiceFromYaml(Cluster cluster, Map<String, Object> yamlMap, int port) {
        // Convert the YAML map to a V1Service object
        V1Service service = Yaml.loadAs(Yaml.dump(yamlMap), V1Service.class);

        // Set the port in the service spec if needed
        Objects.requireNonNull(Objects.requireNonNull(service.getSpec()).getPorts()).forEach(servicePort -> {
            servicePort.setPort(port);
            servicePort.setTargetPort(new IntOrString(port));
        });

        try {
            // Create the service in the specified namespace
            cluster.coreV1Api.createNamespacedService(
                    Objects.requireNonNull(service.getMetadata()).getNamespace(),
                    service,
                    null,
                    null,
                    null,
                    null
            );
            logger.info("Service created from YAML: {}", service.getMetadata().getName());
        } catch (ApiException e) {
            logger.error("Failed to create service from YAML: {}", e.getResponseBody(), e);
        }
    }
}