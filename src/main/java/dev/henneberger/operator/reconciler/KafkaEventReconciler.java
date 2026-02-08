package dev.henneberger.operator.reconciler;

import dev.henneberger.operator.crd.KafkaEvent;
import dev.henneberger.operator.resource.GraphQLSchemaResourceManager;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;

@ControllerConfiguration
public class KafkaEventReconciler implements Reconciler<KafkaEvent> {

  private final GraphQLSchemaResourceManager resourceManager;

  public KafkaEventReconciler(GraphQLSchemaResourceManager resourceManager) {
    this.resourceManager = resourceManager;
  }

  @Override
  public UpdateControl<KafkaEvent> reconcile(KafkaEvent resource, Context<KafkaEvent> context) {
    if (resource.getSpec() != null) {
      resourceManager.ensureResourcesForSchemas(
        resource.getMetadata().getNamespace(),
        resource.getSpec().getSchemaRefs());
    }
    return UpdateControl.noUpdate();
  }
}
