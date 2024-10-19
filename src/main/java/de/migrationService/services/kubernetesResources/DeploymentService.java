package de.migrationService.services.kubernetesResources;

import de.migrationService.models.Cluster;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1ContainerPort;
import io.kubernetes.client.openapi.models.V1Deployment;
import io.kubernetes.client.openapi.models.V1DeploymentList;
import io.kubernetes.client.openapi.models.V1DeploymentSpec;
import io.kubernetes.client.openapi.models.V1LabelSelector;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1PodTemplateSpec;
import io.kubernetes.client.util.Yaml;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Objects;

@Service
public class DeploymentService {

    private static final Logger logger = LoggerFactory.getLogger(DeploymentService.class);

    /**
     * Gets the difference in deployments between two clusters.
     *
     * @param cluster1 The first cluster.
     * @param cluster2 The second cluster.
     * @return A list of deployments that are in the first cluster but not in the second.
     */
    public V1DeploymentList getDiffDeployments(Cluster cluster1, Cluster cluster2) {
        V1DeploymentList diffDeployments = new V1DeploymentList();

        cluster1.deploymentList.getItems().stream()
                .filter(deployment -> cluster2.deploymentList.getItems().stream()
                        .noneMatch(deployment2 -> Objects.equals(Objects.requireNonNull(deployment2.getMetadata()).getName(), Objects.requireNonNull(deployment.getMetadata()).getName())))
                .forEach(deployment -> diffDeployments.getItems().add(deployment));

        logger.info("Deployment diff: ");
        diffDeployments.getItems().forEach(deployment -> logger.info(Objects.requireNonNull(deployment.getMetadata()).getName()));

        return diffDeployments;
    }

    /**
     * Creates deployments in the specified cluster.
     *
     * @param cluster     The cluster where deployments will be created.
     * @param deployments The list of deployments to be created.
     */
    public void createDeployments(Cluster cluster, V1DeploymentList deployments) {
        deployments.getItems().forEach(deployment -> {
            try {
                cluster.appsV1Api.createNamespacedDeployment(
                        Objects.requireNonNull(deployment.getMetadata()).getNamespace(),
                        cleanDeploymentForCreation(deployment),
                        null,
                        null,
                        null,
                        null
                );
                logger.info("Deployment from diff created: {}", Objects.requireNonNull(deployment.getMetadata()).getName());
            } catch (Exception e) {
                logger.error("Error creating deployment: {}", Objects.requireNonNull(deployment.getMetadata()).getName(), e);
            }
        });
    }

    /**
     * Creates a single deployment in the specified cluster.
     *
     * @param cluster   The cluster where the deployment will be created.
     * @param name      The name of the deployment.
     * @param namespace The namespace of the deployment.
     * @param image     The image of the deployment.
     * @param port      The port of the deployment.
     */
    public void createDeployment(Cluster cluster, String name, String namespace, String image, int port) {
        try {
            // Check if the deployment already exists
            V1Deployment existingDeployment = cluster.appsV1Api.readNamespacedDeployment(name, namespace, null);
            if (existingDeployment != null) {
                logger.info("Deployment '{}' already exists in namespace '{}'. Skipping creation.", name, namespace);
                return;
            }
        } catch (ApiException e) {
            // A 404 error indicates that the deployment doesn't exist, so we can proceed with creation.
            if (e.getCode() != 404) {
                logger.error("Error checking for existing deployment: {}", e.getResponseBody(), e);
                throw new RuntimeException(e);
            }
        }

        // Create a container with the specified image and port
        V1Container container = new V1Container()
                .name(name)
                .image(image)
                .addPortsItem(new V1ContainerPort().containerPort(port));

        // Create a pod template that uses the container
        V1PodTemplateSpec podTemplateSpec = new V1PodTemplateSpec()
                .metadata(new V1ObjectMeta().labels(Map.of("app", name)))
                .spec(new V1PodSpec().addContainersItem(container));

        // Create a deployment spec that sets the replicas and the selector
        V1DeploymentSpec deploymentSpec = new V1DeploymentSpec()
                .replicas(1)  // Number of pod replicas
                .selector(new V1LabelSelector().matchLabels(Map.of("app", name)))
                .template(podTemplateSpec);

        // Create the deployment with the specified metadata and spec
        V1Deployment deployment = new V1Deployment()
                .apiVersion("apps/v1")
                .kind("Deployment")
                .metadata(new V1ObjectMeta().name(name).namespace(namespace))
                .spec(deploymentSpec);

        try {
            // Create the deployment in the specified namespace
            cluster.appsV1Api.createNamespacedDeployment(namespace, deployment, null, null, null, null);
            logger.info("Deployment '{}' created in namespace '{}' with image '{}' and port {}", name, namespace, image, port);
        } catch (ApiException e) {
            logger.error("Failed to create deployment: {}", e.getResponseBody(), e);
        }
    }

    /**
     * Cleans a deployment for creation by removing unnecessary fields.
     *
     * @param originalDeployment The original deployment.
     * @return The cleaned deployment.
     */
    private V1Deployment cleanDeploymentForCreation(V1Deployment originalDeployment) {
        V1ObjectMeta originalMetadata = originalDeployment.getMetadata();
        V1DeploymentSpec originalSpec = originalDeployment.getSpec();

        assert originalMetadata != null;
        V1ObjectMeta cleanedMetadata = new V1ObjectMeta()
                .name(originalMetadata.getName())
                .namespace(originalMetadata.getNamespace())
                .labels(originalMetadata.getLabels())
                .annotations(originalMetadata.getAnnotations());

        return new V1Deployment()
                .apiVersion("apps/v1")
                .kind("Deployment")
                .metadata(cleanedMetadata)
                .spec(originalSpec);
    }

    /**
     * Creates a deployment from a YAML map in the specified cluster.
     *
     * @param cluster The cluster where the deployment will be created.
     * @param yamlMap The YAML map representing the deployment.
     * @param port    The port of the deployment.
     */
    public void createDeploymentFromYaml(Cluster cluster, Map<String, Object> yamlMap, int port) {
        // Convert the YAML map to a V1Deployment object
        V1Deployment deployment = Yaml.loadAs(Yaml.dump(yamlMap), V1Deployment.class);

        // Set the port in the container spec if needed
        Objects.requireNonNull(Objects.requireNonNull(deployment.getSpec()).getTemplate().getSpec()).getContainers().forEach(container -> Objects.requireNonNull(container.getPorts()).forEach(containerPort -> containerPort.setContainerPort(port)));

        try {
            // Create the deployment in the specified namespace
            cluster.appsV1Api.createNamespacedDeployment(
                    Objects.requireNonNull(deployment.getMetadata()).getNamespace(),
                    deployment,
                    null,
                    null,
                    null,
                    null
            );
            logger.info("Deployment created from YAML: {}", deployment.getMetadata().getName());
        } catch (ApiException e) {
            logger.error("Failed to create deployment from YAML: {}", e.getResponseBody(), e);
        }
    }
}