package dev.henneberger.operator.reconciler;

import dev.henneberger.operator.crd.PostgresMutation;
import dev.henneberger.operator.resource.GraphQLSchemaResourceManager;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;

@ControllerConfiguration
public class PostgresMutationReconciler implements Reconciler<PostgresMutation> {

  private final GraphQLSchemaResourceManager resourceManager;

  public PostgresMutationReconciler(GraphQLSchemaResourceManager resourceManager) {
    this.resourceManager = resourceManager;
  }

  @Override
  public UpdateControl<PostgresMutation> reconcile(PostgresMutation resource, Context<PostgresMutation> context) {
    resourceManager.ensureResourcesForAllSchemas(resource.getMetadata().getNamespace());
    return UpdateControl.noUpdate();
  }
}
