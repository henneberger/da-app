package dev.henneberger.operator.reconciler;

import dev.henneberger.operator.crd.MongoConnection;
import dev.henneberger.operator.resource.GraphQLSchemaResourceManager;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;

@ControllerConfiguration
public class MongoConnectionReconciler implements Reconciler<MongoConnection> {

  private final GraphQLSchemaResourceManager resourceManager;

  public MongoConnectionReconciler(GraphQLSchemaResourceManager resourceManager) {
    this.resourceManager = resourceManager;
  }

  @Override
  public UpdateControl<MongoConnection> reconcile(MongoConnection resource, Context<MongoConnection> context) {
    resourceManager.ensureResourcesForAllSchemas(resource.getMetadata().getNamespace());
    return UpdateControl.noUpdate();
  }
}
