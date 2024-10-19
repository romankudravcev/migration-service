package de.migrationService.services.kubernetesResources;

import de.migrationService.models.Cluster;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.models.V1Secret;
import io.kubernetes.client.openapi.models.V1SecretList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Objects;

@Service
public class SecretService {

    private static final Logger logger = LoggerFactory.getLogger(SecretService.class);

    /**
     * Gets the difference in secrets between two clusters.
     *
     * @param cluster1 The first cluster.
     * @param cluster2 The second cluster.
     * @return A list of secrets that are in the first cluster but not in the second.
     */
    public V1SecretList getDiffSecrets(Cluster cluster1, Cluster cluster2) {
        V1SecretList diffSecrets = new V1SecretList();

        cluster1.secretList.getItems().stream()
                .filter(secret -> cluster2.secretList.getItems().stream()
                        .noneMatch(secret2 -> Objects.equals(Objects.requireNonNull(secret2.getMetadata()).getName(), Objects.requireNonNull(secret.getMetadata()).getName())))
                .forEach(secret -> diffSecrets.getItems().add(secret));

        logger.info("Secret diff: ");
        diffSecrets.getItems().forEach(secret -> logger.info(Objects.requireNonNull(secret.getMetadata()).getName()));

        return diffSecrets;
    }

    /**
     * Creates secrets in the specified cluster.
     *
     * @param cluster The cluster where secrets will be created.
     * @param secrets The list of secrets to be created.
     */
    public void createSecrets(Cluster cluster, V1SecretList secrets) {
        secrets.getItems().forEach(secret -> {
            try {
                cluster.coreV1Api.createNamespacedSecret(
                        Objects.requireNonNull(secret.getMetadata()).getNamespace(),
                        cleanSecretForCreation(secret),
                        null,
                        null,
                        null,
                        null
                );
                logger.info("Secret from diff created: {}", Objects.requireNonNull(secret.getMetadata()).getName());
            } catch (Exception e) {
                logger.error("Error creating Secret: {}", Objects.requireNonNull(secret.getMetadata()).getName(), e);
            }
        });
    }

    /**
     * Creates a single secret in the specified cluster.
     *
     * @param cluster   The cluster where the secret will be created.
     * @param name      The name of the secret.
     * @param namespace The namespace of the secret.
     * @param data      The data of the secret.
     */
    public void createSecret(Cluster cluster, String name, String namespace, Map<String, String> data) {
        try {
            // Check if the secret already exists
            V1Secret existingSecret = cluster.coreV1Api.readNamespacedSecret(name, namespace, null);
            if (existingSecret != null) {
                logger.info("Secret '{}' already exists in namespace '{}'. Skipping creation.", name, namespace);
                return;
            }
        } catch (ApiException e) {
            // A 404 error indicates that the secret doesn't exist, so we can proceed with creation.
            if (e.getCode() != 404) {
                logger.error("Error checking for existing secret: {}", e.getResponseBody(), e);
                throw new RuntimeException(e);
            }
        }

        // Create the secret if it doesn't exist
        V1Secret secret = new V1Secret()
                .metadata(new io.kubernetes.client.openapi.models.V1ObjectMeta().name(name).namespace(namespace))
                .stringData(data);

        try {
            cluster.coreV1Api.createNamespacedSecret(namespace, secret, null, null, null, null);
            logger.info("Secret created: {}", name);
        } catch (Exception e) {
            logger.error("Error creating Secret: {}", name, e);
        }
    }

    /**
     * Cleans a secret for creation by removing unnecessary fields.
     *
     * @param originalSecret The original secret.
     * @return The cleaned secret.
     */
    private V1Secret cleanSecretForCreation(V1Secret originalSecret) {
        return new V1Secret()
                .apiVersion("v1")
                .kind("Secret")
                .metadata(originalSecret.getMetadata())
                .stringData(originalSecret.getStringData());
    }
}