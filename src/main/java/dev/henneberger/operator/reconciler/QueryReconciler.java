package dev.henneberger.operator.reconciler;

import dev.henneberger.operator.crd.Query;
import dev.henneberger.operator.resource.GraphQLSchemaResourceManager;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;

@ControllerConfiguration
public class QueryReconciler implements Reconciler<Query> {

  private final GraphQLSchemaResourceManager resourceManager;

  public QueryReconciler(GraphQLSchemaResourceManager resourceManager) {
    this.resourceManager = resourceManager;
  }

  @Override
  public UpdateControl<Query> reconcile(Query resource, Context<Query> context) {
    if (resource.getSpec() != null) {
      resourceManager.ensureResourcesForSchemas(
        resource.getMetadata().getNamespace(),
        resource.getSpec().getSchemaRefs());
    }
    return UpdateControl.noUpdate();
  }
}
