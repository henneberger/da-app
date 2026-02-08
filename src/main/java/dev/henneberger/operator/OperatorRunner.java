package dev.henneberger.operator;

import dev.henneberger.operator.reconciler.ConnectionReconciler;
import dev.henneberger.operator.reconciler.DuckDBConnectionReconciler;
import dev.henneberger.operator.reconciler.DynamoDbConnectionReconciler;
import dev.henneberger.operator.reconciler.ElasticsearchConnectionReconciler;
import dev.henneberger.operator.reconciler.GraphQLOperationReconciler;
import dev.henneberger.operator.reconciler.GraphQLSchemaReconciler;
import dev.henneberger.operator.reconciler.IngestionEndpointReconciler;
import dev.henneberger.operator.reconciler.JdbcConnectionReconciler;
import dev.henneberger.operator.reconciler.JsonSchemaReconciler;
import dev.henneberger.operator.reconciler.KafkaConnectionReconciler;
import dev.henneberger.operator.reconciler.KafkaEventReconciler;
import dev.henneberger.operator.reconciler.KafkaSubscriptionReconciler;
import dev.henneberger.operator.reconciler.McpServerReconciler;
import dev.henneberger.operator.reconciler.MongoConnectionReconciler;
import dev.henneberger.operator.reconciler.PostgresMutationReconciler;
import dev.henneberger.operator.reconciler.PostgresSubscriptionReconciler;
import dev.henneberger.operator.reconciler.PythonFunctionReconciler;
import dev.henneberger.operator.reconciler.QueryReconciler;
import dev.henneberger.operator.reconciler.RestGraphQLEndpointReconciler;
import dev.henneberger.operator.FlinkSqlJobReconciler;
import dev.henneberger.operator.resource.GraphQLSchemaResourceManager;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.javaoperatorsdk.operator.Operator;

public final class OperatorRunner {

  private OperatorRunner() {
  }

  public static void start() {
    KubernetesClient client = new KubernetesClientBuilder().build();
    GraphQLSchemaResourceManager resourceManager = new GraphQLSchemaResourceManager(client);

    Operator operator = new Operator(client, null);
    operator.register(new ConnectionReconciler(resourceManager));
    operator.register(new DuckDBConnectionReconciler(resourceManager));
    operator.register(new MongoConnectionReconciler(resourceManager));
    operator.register(new ElasticsearchConnectionReconciler(resourceManager));
    operator.register(new JdbcConnectionReconciler(resourceManager));
    operator.register(new DynamoDbConnectionReconciler(resourceManager));
    operator.register(new QueryReconciler(resourceManager));
    operator.register(new PythonFunctionReconciler(resourceManager));
    operator.register(new GraphQLOperationReconciler(resourceManager));
    operator.register(new RestGraphQLEndpointReconciler(resourceManager));
    operator.register(new McpServerReconciler(resourceManager));
    operator.register(new GraphQLSchemaReconciler(resourceManager));
    operator.register(new KafkaConnectionReconciler(resourceManager));
    operator.register(new KafkaEventReconciler(resourceManager));
    operator.register(new PostgresMutationReconciler(resourceManager));
    operator.register(new KafkaSubscriptionReconciler(resourceManager));
    operator.register(new PostgresSubscriptionReconciler(resourceManager));
    operator.register(new IngestionEndpointReconciler(resourceManager));
    operator.register(new JsonSchemaReconciler(resourceManager));
    operator.register(new FlinkSqlJobReconciler(client));
    operator.start();
  }
}
