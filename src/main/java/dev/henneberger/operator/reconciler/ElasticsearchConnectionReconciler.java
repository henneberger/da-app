package dev.henneberger.operator.reconciler;

import dev.henneberger.operator.crd.ElasticsearchConnection;
import dev.henneberger.operator.resource.GraphQLSchemaResourceManager;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;

@ControllerConfiguration
public class ElasticsearchConnectionReconciler implements Reconciler<ElasticsearchConnection> {

  private final GraphQLSchemaResourceManager resourceManager;

  public ElasticsearchConnectionReconciler(GraphQLSchemaResourceManager resourceManager) {
    this.resourceManager = resourceManager;
  }

  @Override
  public UpdateControl<ElasticsearchConnection> reconcile(ElasticsearchConnection resource,
                                                          Context<ElasticsearchConnection> context) {
    resourceManager.ensureResourcesForAllSchemas(resource.getMetadata().getNamespace());
    return UpdateControl.noUpdate();
  }
}
