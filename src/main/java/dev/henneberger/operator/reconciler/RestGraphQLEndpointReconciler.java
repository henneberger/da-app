package dev.henneberger.operator.reconciler;

import dev.henneberger.operator.crd.RestGraphQLEndpoint;
import dev.henneberger.operator.resource.GraphQLSchemaResourceManager;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;

@ControllerConfiguration
public class RestGraphQLEndpointReconciler implements Reconciler<RestGraphQLEndpoint> {
  private final GraphQLSchemaResourceManager resourceManager;

  public RestGraphQLEndpointReconciler(GraphQLSchemaResourceManager resourceManager) {
    this.resourceManager = resourceManager;
  }

  @Override
  public UpdateControl<RestGraphQLEndpoint> reconcile(RestGraphQLEndpoint resource, Context<RestGraphQLEndpoint> context) {
    if (resource.getSpec() != null) {
      resourceManager.ensureResourcesForSchemas(
        resource.getMetadata().getNamespace(),
        resource.getSpec().getSchemaRefs());
    }
    return UpdateControl.noUpdate();
  }
}

