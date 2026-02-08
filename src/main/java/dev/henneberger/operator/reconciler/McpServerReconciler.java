package dev.henneberger.operator.reconciler;

import dev.henneberger.operator.crd.McpServer;
import dev.henneberger.operator.resource.GraphQLSchemaResourceManager;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;

@ControllerConfiguration
public class McpServerReconciler implements Reconciler<McpServer> {
  private final GraphQLSchemaResourceManager resourceManager;

  public McpServerReconciler(GraphQLSchemaResourceManager resourceManager) {
    this.resourceManager = resourceManager;
  }

  @Override
  public UpdateControl<McpServer> reconcile(McpServer resource, Context<McpServer> context) {
    if (resource.getSpec() != null) {
      resourceManager.ensureResourcesForSchemas(
        resource.getMetadata().getNamespace(),
        resource.getSpec().getSchemaRefs());
    }
    return UpdateControl.noUpdate();
  }
}

