package dev.henneberger.operator.reconciler;

import dev.henneberger.operator.crd.IngestionEndpoint;
import dev.henneberger.operator.resource.GraphQLSchemaResourceManager;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;

@ControllerConfiguration
public class IngestionEndpointReconciler implements Reconciler<IngestionEndpoint> {

  private final GraphQLSchemaResourceManager resourceManager;

  public IngestionEndpointReconciler(GraphQLSchemaResourceManager resourceManager) {
    this.resourceManager = resourceManager;
  }

  @Override
  public UpdateControl<IngestionEndpoint> reconcile(IngestionEndpoint resource,
                                                    Context<IngestionEndpoint> context) {
    if (resource.getSpec() != null) {
      resourceManager.ensureResourcesForSchemas(
        resource.getMetadata().getNamespace(),
        resource.getSpec().getSchemaRefs());
    }
    return UpdateControl.noUpdate();
  }
}
