package dev.henneberger.operator.reconciler;

import dev.henneberger.operator.crd.JsonSchema;
import dev.henneberger.operator.resource.GraphQLSchemaResourceManager;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;

@ControllerConfiguration
public class JsonSchemaReconciler implements Reconciler<JsonSchema> {

  private final GraphQLSchemaResourceManager resourceManager;

  public JsonSchemaReconciler(GraphQLSchemaResourceManager resourceManager) {
    this.resourceManager = resourceManager;
  }

  @Override
  public UpdateControl<JsonSchema> reconcile(JsonSchema resource, Context<JsonSchema> context) {
    resourceManager.ensureResourcesForAllSchemas(resource.getMetadata().getNamespace());
    return UpdateControl.noUpdate();
  }
}
