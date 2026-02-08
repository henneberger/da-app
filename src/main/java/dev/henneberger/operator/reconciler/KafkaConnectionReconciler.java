package dev.henneberger.operator.reconciler;

import dev.henneberger.operator.crd.KafkaConnection;
import dev.henneberger.operator.resource.GraphQLSchemaResourceManager;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;

@ControllerConfiguration
public class KafkaConnectionReconciler implements Reconciler<KafkaConnection> {

  private final GraphQLSchemaResourceManager resourceManager;

  public KafkaConnectionReconciler(GraphQLSchemaResourceManager resourceManager) {
    this.resourceManager = resourceManager;
  }

  @Override
  public UpdateControl<KafkaConnection> reconcile(KafkaConnection resource, Context<KafkaConnection> context) {
    resourceManager.ensureResourcesForAllSchemas(resource.getMetadata().getNamespace());
    return UpdateControl.noUpdate();
  }
}
