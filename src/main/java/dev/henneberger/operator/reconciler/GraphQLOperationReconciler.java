package dev.henneberger.operator.reconciler;

import dev.henneberger.operator.crd.GraphQLOperation;
import dev.henneberger.operator.resource.GraphQLSchemaResourceManager;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;

@ControllerConfiguration
public class GraphQLOperationReconciler implements Reconciler<GraphQLOperation> {
  private final GraphQLSchemaResourceManager resourceManager;

  public GraphQLOperationReconciler(GraphQLSchemaResourceManager resourceManager) {
    this.resourceManager = resourceManager;
  }

  @Override
  public UpdateControl<GraphQLOperation> reconcile(GraphQLOperation resource, Context<GraphQLOperation> context) {
    if (resource.getSpec() != null) {
      resourceManager.ensureResourcesForSchemas(
        resource.getMetadata().getNamespace(),
        resource.getSpec().getSchemaRefs());
    }
    return UpdateControl.noUpdate();
  }
}

