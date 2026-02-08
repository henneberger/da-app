package dev.henneberger.operator.reconciler;

import dev.henneberger.operator.crd.PostgresSubscription;
import dev.henneberger.operator.resource.GraphQLSchemaResourceManager;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;

@ControllerConfiguration
public class PostgresSubscriptionReconciler implements Reconciler<PostgresSubscription> {

  private final GraphQLSchemaResourceManager resourceManager;

  public PostgresSubscriptionReconciler(GraphQLSchemaResourceManager resourceManager) {
    this.resourceManager = resourceManager;
  }

  @Override
  public UpdateControl<PostgresSubscription> reconcile(PostgresSubscription resource,
                                                       Context<PostgresSubscription> context) {
    resourceManager.ensureResourcesForAllSchemas(resource.getMetadata().getNamespace());
    return UpdateControl.noUpdate();
  }
}
