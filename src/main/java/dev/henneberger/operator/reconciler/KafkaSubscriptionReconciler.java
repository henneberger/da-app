package dev.henneberger.operator.reconciler;

import dev.henneberger.operator.crd.KafkaSubscription;
import dev.henneberger.operator.resource.GraphQLSchemaResourceManager;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;

@ControllerConfiguration
public class KafkaSubscriptionReconciler implements Reconciler<KafkaSubscription> {

  private final GraphQLSchemaResourceManager resourceManager;

  public KafkaSubscriptionReconciler(GraphQLSchemaResourceManager resourceManager) {
    this.resourceManager = resourceManager;
  }

  @Override
  public UpdateControl<KafkaSubscription> reconcile(KafkaSubscription resource,
                                                    Context<KafkaSubscription> context) {
    if (resource.getSpec() != null) {
      resourceManager.ensureResourcesForSchemas(
        resource.getMetadata().getNamespace(),
        resource.getSpec().getSchemaRefs());
    }
    return UpdateControl.noUpdate();
  }
}
