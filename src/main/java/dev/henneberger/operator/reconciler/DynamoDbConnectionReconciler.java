package dev.henneberger.operator.reconciler;

import dev.henneberger.operator.crd.DynamoDbConnection;
import dev.henneberger.operator.resource.GraphQLSchemaResourceManager;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;

@ControllerConfiguration
public class DynamoDbConnectionReconciler implements Reconciler<DynamoDbConnection> {

  private final GraphQLSchemaResourceManager resourceManager;

  public DynamoDbConnectionReconciler(GraphQLSchemaResourceManager resourceManager) {
    this.resourceManager = resourceManager;
  }

  @Override
  public UpdateControl<DynamoDbConnection> reconcile(DynamoDbConnection resource,
                                                    Context<DynamoDbConnection> context) {
    resourceManager.ensureResourcesForAllSchemas(resource.getMetadata().getNamespace());
    return UpdateControl.noUpdate();
  }
}
