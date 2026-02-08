package dev.henneberger.operator.reconciler;

import dev.henneberger.operator.crd.JdbcConnection;
import dev.henneberger.operator.resource.GraphQLSchemaResourceManager;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;

@ControllerConfiguration
public class JdbcConnectionReconciler implements Reconciler<JdbcConnection> {

  private final GraphQLSchemaResourceManager resourceManager;

  public JdbcConnectionReconciler(GraphQLSchemaResourceManager resourceManager) {
    this.resourceManager = resourceManager;
  }

  @Override
  public UpdateControl<JdbcConnection> reconcile(JdbcConnection resource, Context<JdbcConnection> context) {
    resourceManager.ensureResourcesForAllSchemas(resource.getMetadata().getNamespace());
    return UpdateControl.noUpdate();
  }
}
