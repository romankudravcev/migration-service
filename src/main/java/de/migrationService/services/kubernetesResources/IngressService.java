package de.migrationService.services.kubernetesResources;

import de.migrationService.models.Cluster;
import io.kubernetes.client.openapi.models.V1Ingress;
import io.kubernetes.client.openapi.models.V1IngressList;
import io.kubernetes.client.openapi.models.V1IngressSpec;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Objects;

@Service
public class IngressService {

    private static final Logger logger = LoggerFactory.getLogger(IngressService.class);

    /**
     * Gets the difference in ingresses between two clusters.
     *
     * @param cluster1 The first cluster.
     * @param cluster2 The second cluster.
     * @return A list of ingresses that are in the first cluster but not in the second.
     */
    public V1IngressList getDiffIngress(Cluster cluster1, Cluster cluster2) {
        V1IngressList diffIngresses = new V1IngressList();

        cluster1.ingressList.getItems().stream()
                .filter(ingress -> cluster2.ingressList.getItems().stream()
                        .noneMatch(ingress2 -> Objects.equals(Objects.requireNonNull(ingress2.getMetadata()).getName(), Objects.requireNonNull(ingress.getMetadata()).getName())))
                .forEach(deployment -> diffIngresses.getItems().add(deployment));

        logger.info("Ingress diff: ");
        diffIngresses.getItems().forEach(ingress -> logger.info(Objects.requireNonNull(ingress.getMetadata()).getName()));

        return diffIngresses;
    }

    /**
     * Creates ingresses in the specified cluster.
     *
     * @param cluster   The cluster where ingresses will be created.
     * @param ingresses The list of ingresses to be created.
     */
    public void createIngress(Cluster cluster, V1IngressList ingresses) {
        ingresses.getItems().forEach(ingress -> {
            try {
                cluster.networkingV1Api.createNamespacedIngress(
                        Objects.requireNonNull(ingress.getMetadata()).getNamespace(),
                        cleanIngressForCreation(ingress),
                        null, null, null, null);
                logger.info("Ingress created in target cluster: {}", ingress.getMetadata().getName());
            } catch (Exception e) {
                logger.error("Error creating Ingress in target cluster: {}", Objects.requireNonNull(ingress.getMetadata()).getName(), e);
            }
        });
    }

    /**
     * Cleans an ingress for creation by removing unnecessary fields.
     *
     * @param originalIngress The original ingress.
     * @return The cleaned ingress.
     */
    private V1Ingress cleanIngressForCreation(V1Ingress originalIngress) {
        V1ObjectMeta originalMetadata = originalIngress.getMetadata();
        V1IngressSpec originalSpec = originalIngress.getSpec();

        assert originalMetadata != null;
        V1ObjectMeta cleanedMetadata = new V1ObjectMeta()
                .name(originalMetadata.getName())
                .namespace(originalMetadata.getNamespace())
                .labels(originalMetadata.getLabels())
                .annotations(originalMetadata.getAnnotations());

        return new V1Ingress()
                .apiVersion("networking.k8s.io/v1")
                .kind("Ingress")
                .metadata(cleanedMetadata)
                .spec(originalSpec);
    }
}