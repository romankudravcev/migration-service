package de.migrationService.services.kubernetesResources;

import de.migrationService.models.Cluster;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.models.V1Namespace;
import io.kubernetes.client.openapi.models.V1NamespaceList;
import io.kubernetes.client.openapi.models.V1NamespaceSpec;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Objects;

@Service
public class NamespaceService {

    private static final Logger logger = LoggerFactory.getLogger(NamespaceService.class);

    /**
     * Gets the difference in namespaces between two clusters.
     *
     * @param cluster1 The first cluster.
     * @param cluster2 The second cluster.
     * @return A list of namespaces that are in the first cluster but not in the second.
     */
    public V1NamespaceList getDiffNamespaces(Cluster cluster1, Cluster cluster2) {
        V1NamespaceList diffNamespaces = new V1NamespaceList();

        cluster1.namespaceList.getItems().stream()
                .filter(namespace -> cluster2.namespaceList.getItems().stream()
                        .noneMatch(namespace2 -> Objects.equals(Objects.requireNonNull(namespace2.getMetadata()).getName(), Objects.requireNonNull(namespace.getMetadata()).getName())))
                .forEach(namespace -> diffNamespaces.getItems().add(namespace));

        logger.info("Namespace diff: ");
        diffNamespaces.getItems().forEach(namespace -> logger.info(Objects.requireNonNull(namespace.getMetadata()).getName()));

        return diffNamespaces;
    }

    /**
     * Creates namespaces in the specified cluster.
     *
     * @param cluster    The cluster where namespaces will be created.
     * @param namespaces The list of namespaces to be created.
     */
    public void createNamespaces(Cluster cluster, V1NamespaceList namespaces) {
        namespaces.getItems().forEach(namespace -> {
            try {
                cluster.coreV1Api.createNamespace(cleanNamespaceForCreation(namespace), null, null, null, null);
                logger.info("Namespace from diff created: {}", Objects.requireNonNull(namespace.getMetadata()).getName());
            } catch (Exception e) {
                logger.error("Error creating namespace: {}", Objects.requireNonNull(namespace.getMetadata()).getName(), e);
            }
        });
    }

    /**
     * Creates a single namespace in the specified cluster.
     *
     * @param cluster The cluster where the namespace will be created.
     * @param name    The name of the namespace.
     */
    public void createNamespace(Cluster cluster, String name) {
        try {
            // Check if the namespace already exists
            V1Namespace existingNamespace = cluster.coreV1Api.readNamespace(name, null);
            if (existingNamespace != null) {
                logger.info("Namespace '{}' already exists. Skipping creation.", name);
                return;
            }
        } catch (ApiException e) {
            // A 404 error indicates that the namespace doesn't exist, so we can proceed with creation.
            if (e.getCode() != 404) {
                logger.error("Error checking for existing namespace: {}", e.getResponseBody(), e);
                throw new RuntimeException(e);
            }
        }

        // Create the namespace if it doesn't exist
        V1Namespace namespace = new V1Namespace()
                .metadata(new V1ObjectMeta().name(name));
        try {
            cluster.coreV1Api.createNamespace(namespace, null, null, null, null);
            logger.info("Namespace '{}' created successfully.", name);
        } catch (ApiException e) {
            logger.error("Error while creating namespace '{}'", name, e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Cleans a namespace for creation by removing unnecessary fields.
     *
     * @param originalNamespace The original namespace.
     * @return The cleaned namespace.
     */
    public V1Namespace cleanNamespaceForCreation(V1Namespace originalNamespace) {
        V1ObjectMeta originalMetadata = originalNamespace.getMetadata();
        V1NamespaceSpec originalSpec = originalNamespace.getSpec();

        assert originalMetadata != null;
        V1ObjectMeta cleanedMetadata = new V1ObjectMeta()
                .name(originalMetadata.getName())
                .labels(originalMetadata.getLabels())
                .annotations(originalMetadata.getAnnotations());

        return new V1Namespace()
                .apiVersion("v1")
                .kind("Namespace")
                .metadata(cleanedMetadata)
                .spec(originalSpec);
    }
}