package dev.henneberger.operator.reconciler;

import dev.henneberger.operator.crd.DuckDBConnection;
import dev.henneberger.operator.resource.GraphQLSchemaResourceManager;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;

@ControllerConfiguration
public class DuckDBConnectionReconciler implements Reconciler<DuckDBConnection> {

  private final GraphQLSchemaResourceManager resourceManager;

  public DuckDBConnectionReconciler(GraphQLSchemaResourceManager resourceManager) {
    this.resourceManager = resourceManager;
  }

  @Override
  public UpdateControl<DuckDBConnection> reconcile(DuckDBConnection resource, Context<DuckDBConnection> context) {
    resourceManager.ensureResourcesForAllSchemas(resource.getMetadata().getNamespace());
    return UpdateControl.noUpdate();
  }
}
