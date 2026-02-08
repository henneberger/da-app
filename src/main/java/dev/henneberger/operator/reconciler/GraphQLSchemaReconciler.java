package dev.henneberger.operator.reconciler;

import dev.henneberger.operator.crd.GraphQLSchema;
import dev.henneberger.operator.resource.GraphQLSchemaResourceManager;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;

@ControllerConfiguration
public class GraphQLSchemaReconciler implements Reconciler<GraphQLSchema> {

  private final GraphQLSchemaResourceManager resourceManager;

  public GraphQLSchemaReconciler(GraphQLSchemaResourceManager resourceManager) {
    this.resourceManager = resourceManager;
  }

  @Override
  public UpdateControl<GraphQLSchema> reconcile(GraphQLSchema resource, Context<GraphQLSchema> context) {
    resourceManager.ensureResources(resource);
    return UpdateControl.noUpdate();
  }
}
