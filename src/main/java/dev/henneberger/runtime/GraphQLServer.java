package dev.henneberger.runtime;

import dev.henneberger.runtime.config.AddFieldConfig;
import dev.henneberger.runtime.config.AuthConfig;
import dev.henneberger.runtime.config.BasicAuthUserConfig;
import dev.henneberger.runtime.config.ConnectionConfig;
import dev.henneberger.runtime.config.DynamoDbConnectionConfig;
import dev.henneberger.runtime.config.DuckDbConnectionConfig;
import dev.henneberger.runtime.config.EnrichFieldConfig;
import dev.henneberger.runtime.config.ElasticsearchConnectionConfig;
import dev.henneberger.runtime.config.IngestionEndpointConfig;
import dev.henneberger.runtime.config.JdbcConnectionConfig;
import dev.henneberger.runtime.config.JsonSchemaConfig;
import dev.henneberger.runtime.config.JwtAuthConfig;
import dev.henneberger.runtime.config.KafkaConnectionConfig;
import dev.henneberger.runtime.config.KafkaEventConfig;
import dev.henneberger.runtime.config.KafkaSubscriptionConfig;
import dev.henneberger.runtime.config.KafkaSubscriptionFilterConfig;
import dev.henneberger.runtime.config.MongoConnectionConfig;
import dev.henneberger.runtime.config.MongoQueryConfig;
import dev.henneberger.runtime.config.ParamSourceConfig;
import dev.henneberger.runtime.config.CursorSigningConfig;
import dev.henneberger.runtime.config.PaginationConfig;
import dev.henneberger.runtime.config.ObservabilityConfig;
import dev.henneberger.runtime.config.MetricsConfig;
import dev.henneberger.runtime.config.OtlpMetricsConfig;
import dev.henneberger.runtime.config.TracingConfig;
import dev.henneberger.runtime.config.OtlpTracingConfig;
import dev.henneberger.runtime.config.SamplingConfig;
import dev.henneberger.runtime.config.HealthConfig;
import dev.henneberger.runtime.config.HealthCheckConfig;
import dev.henneberger.runtime.config.PostgresMutationConfig;
import dev.henneberger.runtime.config.PostgresSubscriptionConfig;
import dev.henneberger.runtime.config.PostgresSubscriptionFilterConfig;
import dev.henneberger.runtime.config.QueryConfig;
import dev.henneberger.runtime.config.QueryParamConfig;
import dev.henneberger.runtime.config.RuntimeConfig;
import dev.henneberger.runtime.config.McpServerConfig;
import dev.henneberger.runtime.config.ElasticsearchQueryConfig;
import dev.henneberger.runtime.config.DynamoDbQueryConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.networknt.schema.JsonSchema;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.JWSVerifier;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jose.crypto.MACVerifier;
import com.nimbusds.jose.crypto.factories.DefaultJWSVerifierFactory;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKMatcher;
import com.nimbusds.jose.jwk.JWKSelector;
import com.nimbusds.jose.jwk.KeyUse;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.jwk.OctetKeyPair;
import com.nimbusds.jose.jwk.source.DefaultJWKSetCache;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.jwk.source.RemoteJWKSet;
import com.nimbusds.jose.proc.JWSVerifierFactory;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jose.util.DefaultResourceRetriever;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.GraphQLContext;
import graphql.scalars.ExtendedScalars;
import graphql.schema.GraphQLSchema;
import graphql.execution.SubscriptionExecutionStrategy;
import graphql.language.FieldDefinition;
import graphql.language.ObjectTypeDefinition;
import graphql.language.OperationDefinition;
import graphql.language.VariableDefinition;
import graphql.language.Type;
import graphql.language.NonNullType;
import graphql.language.ListType;
import graphql.language.TypeName;
import graphql.schema.DataFetchingEnvironment;
import graphql.execution.instrumentation.SimpleInstrumentation;
import graphql.execution.instrumentation.InstrumentationState;
import graphql.execution.instrumentation.parameters.InstrumentationExecutionParameters;
import graphql.ExecutionResultImpl;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import graphql.schema.idl.TypeRuntimeWiring;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.registry.otlp.OtlpConfig;
import io.micrometer.registry.otlp.OtlpMeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.MultiMap;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.healthchecks.HealthCheckHandler;
import io.vertx.ext.healthchecks.HealthChecks;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;
import io.vertx.micrometer.MetricsDomain;
import io.vertx.micrometer.backends.BackendRegistries;
import io.vertx.tracing.opentelemetry.OpenTelemetryOptions;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.graphql.GraphQLHandler;
import io.vertx.ext.web.handler.graphql.GraphiQLHandler;
import io.vertx.ext.web.handler.graphql.ws.GraphQLWSHandler;
import io.vertx.ext.web.handler.graphql.ws.Message;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.security.PublicKey;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Objects;
import java.util.Locale;
import java.util.Base64;
import java.util.Properties;
import java.util.UUID;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.net.URLEncoder;
import java.util.Date;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporterBuilder;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporterBuilder;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import com.jayway.jsonpath.JsonPath;
import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

public final class GraphQLServer {
  private static final Logger LOGGER = LoggerFactory.getLogger(GraphQLServer.class);
  private static final String PAGINATION_CONTEXT_KEY = "pagination";
  private static final Map<String, CursorSigner> PAGINATION_SIGNERS = new ConcurrentHashMap<>();
  private static final ObjectMapper CURSOR_MAPPER = new ObjectMapper();
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
  private static volatile PythonFunctionManager PYTHON_FUNCTIONS = new PythonFunctionManager(null);
  private static final SecureRandom OAUTH_RAND = new SecureRandom();
  private static final Map<String, OAuthClient> OAUTH_CLIENTS = new ConcurrentHashMap<>();
  private static final Map<String, OAuthAuthCode> OAUTH_CODES = new ConcurrentHashMap<>();
  private static final Map<String, OAuthRefreshToken> OAUTH_REFRESH = new ConcurrentHashMap<>();

  private GraphQLServer() {
  }

  public static void start(RuntimeConfig config) {
    Objects.requireNonNull(config, "runtime config is required");

    Vertx vertx = Vertx.vertx(buildVertxOptions(config));
    PYTHON_FUNCTIONS = new PythonFunctionManager(config.getPythonFunctions());
    Router router = Router.router(vertx);
    // Request logging: keep this early so we see all traffic (including 401/404/etc).
    router.route().handler(ctx -> {
      long startNs = System.nanoTime();
      String rid = UUID.randomUUID().toString();
      ctx.put("rid", rid);
      String method = ctx.request() == null || ctx.request().method() == null ? "?" : ctx.request().method().name();
      String path = ctx.request() == null ? "?" : ctx.request().path();
      String remote = ctx.request() == null || ctx.request().remoteAddress() == null
        ? "?"
        : String.valueOf(ctx.request().remoteAddress());
      LOGGER.info("HTTP rid={} {} {} from={}", rid, method, path, remote);
      ctx.addBodyEndHandler(ignored -> {
        long ms = (System.nanoTime() - startNs) / 1_000_000L;
        int status = ctx.response() == null ? 0 : ctx.response().getStatusCode();
        LOGGER.info("HTTP rid={} {} {} -> {} in {}ms", rid, method, path, status, ms);
      });
      ctx.next();
    });
    router.route().handler(
      CorsHandler.create("*")
        .allowedMethod(HttpMethod.GET)
        .allowedMethod(HttpMethod.POST)
        .allowedMethod(HttpMethod.OPTIONS)
        .allowedHeader("Content-Type")
        .allowedHeader("Authorization")
        .allowedHeader("Accept")
        .allowedHeader("Origin")
    );
    router.route().handler(BodyHandler.create());

    Map<String, KafkaProducer<String, String>> kafkaProducers = buildKafkaProducers(vertx, config.getKafkaConnections());
    Map<String, KafkaEventConfig> kafkaEvents = buildKafkaEventMap(config.getKafkaEvents());
    Map<String, JsonSchema> jsonSchemaIndex = buildJsonSchemaIndex(config.getJsonSchemas());

    registerIngestionEndpoints(router, config.getIngestionEndpoints(), kafkaProducers, jsonSchemaIndex);

    Map<String, dev.henneberger.runtime.config.GraphQLOperationConfig> graphQLOperations =
      indexGraphQLOperations(config.getGraphQLOperations());

    Map<String, PgPool> pools = buildPgPools(vertx, config.getConnections());
    Map<String, ConnectionConfig> connectionIndex = indexConnections(config.getConnections());
    Map<String, DuckDbConnectionConfig> duckdbConnections = indexDuckDbConnections(config.getDuckdbConnections());
    Map<String, JdbcConnectionConfig> jdbcConnections = indexJdbcConnections(config.getJdbcConnections());
    Map<String, MongoConnectionConfig> mongoConnections = indexMongoConnections(config.getMongoConnections());
    Map<String, ElasticsearchConnectionConfig> elasticsearchConnections =
      indexElasticsearchConnections(config.getElasticsearchConnections());
    Map<String, DynamoDbConnectionConfig> dynamodbConnections =
      indexDynamoDbConnections(config.getDynamodbConnections());
    Map<String, MongoClient> mongoClients = buildMongoClients(mongoConnections);
    Map<String, RestClient> elasticsearchClients = buildElasticsearchClients(elasticsearchConnections);
    Map<String, DynamoDbClient> dynamodbClients = buildDynamoDbClients(dynamodbConnections);
    validateQueryConnections(config.getQueries(), pools, duckdbConnections, jdbcConnections, mongoConnections,
      elasticsearchConnections, dynamodbConnections);
    validatePostgresMutations(config.getPostgresMutations(), pools);
    validatePostgresSubscriptions(config.getPostgresSubscriptions(), connectionIndex);
    PostgresReplicationManager replicationManager = new PostgresReplicationManager(vertx, connectionIndex);
    AuthManager authManager = AuthManager.fromConfig(config.getAuth());
    configurePagination(config.getQueries(), config.getAuth());
    registerOauthEndpoints(router, authManager);

    GraphQL graphQL = buildGraphQL(
      vertx,
      pools,
      duckdbConnections,
      jdbcConnections,
      mongoConnections,
      mongoClients,
      elasticsearchConnections,
      elasticsearchClients,
      dynamodbConnections,
      dynamodbClients,
      config.getSchema(),
      config.getQueries(),
      config.getPostgresMutations(),
      config.getPostgresSubscriptions(),
      replicationManager,
      kafkaProducers,
      kafkaEvents,
      config.getKafkaSubscriptions(),
      config.getKafkaConnections(),
      config.getAuth());
    if (graphQL != null) {
      Route route = router.route("/graphql");
      if (authManager.isEnabled()) {
        route.handler(ctx -> {
          AuthResult result = authManager.authenticateHttp(ctx);
          if (!result.isAllowed()) {
            int status = result.isMissingToken() ? 401 : 403;
            ctx.response().setStatusCode(status).end(result.getMessage());
            return;
          }
          ctx.put("auth", result.getContext());
          ctx.next();
        });
      }
      if (hasSubscriptions(config.getKafkaSubscriptions())
        || hasPostgresSubscriptions(config.getPostgresSubscriptions())) {
        Map<ServerWebSocket, AuthContext> wsAuthContexts = new ConcurrentHashMap<>();

        GraphQLWSHandler wsHandler = GraphQLWSHandler.create(graphQL)
          .connectionInitHandler(event -> {
            LOGGER.info("WS connection init");
            if (authManager.isEnabled()) {
              AuthResult result = authManager.authenticateWebSocket(event.message());
              if (!result.isAllowed()) {
                LOGGER.warn("WS auth failed: {}", result.getMessage());
                event.fail(result.getMessage());
                return;
              }
              wsAuthContexts.put(event.message().socket(), result.getContext());
            }
            event.complete();
          })
          .messageHandler(message -> LOGGER.info("WS message type={}", message.type()))
          .beforeExecute(builderWithContext -> {
            Message message = builderWithContext.context();
            Map<String, Object> contextBuilder = new HashMap<>();
            contextBuilder.put("headers", message.socket().headers());
            AuthContext authContext = wsAuthContexts.get(message.socket());
            if (authContext != null) {
              contextBuilder.put("auth", authContext);
            }
            builderWithContext.builder().graphQLContext(contextBuilder);
          })
          .endHandler(socket -> {
            wsAuthContexts.remove(socket);
            LOGGER.info("WS connection closed id={}", socket.textHandlerID());
          });
        route.handler(wsHandler);
      }
      GraphQLHandler graphQLHandler = GraphQLHandler.create(graphQL)
        .queryContext(rc -> {
          Map<String, Object> context = new HashMap<>();
          context.put("headers", rc.request().headers());
          AuthContext authContext = rc.get("auth");
          if (authContext != null) {
            context.put("auth", authContext);
          }
          return context;
        });
      route.handler(graphQLHandler);
    }

    registerRestGraphQLEndpoints(router, config.getRestGraphQLEndpoints(), graphQLOperations, graphQL, authManager);
    registerMcpServers(router, config.getMcpServers(), graphQLOperations, graphQL, authManager);
    registerMcpGatewayBackend(router, graphQLOperations, graphQL, authManager);

    router.get("/health").handler(ctx -> ctx.response().end("ok"));

    configureObservabilityRoutes(
      vertx,
      router,
      config.getObservability(),
      pools,
      duckdbConnections,
      indexKafkaConnections(config.getKafkaConnections()));

    if ((!pools.isEmpty() || !duckdbConnections.isEmpty()) && isGraphiQLEnabled()) {
      router.route("/graphiql/*").handler(GraphiQLHandler.create());
    }

    int port = Integer.parseInt(System.getenv().getOrDefault("HTTP_PORT", "8080"));
    HttpServerOptions httpServerOptions =
        new HttpServerOptions().addWebSocketSubProtocol("graphql-transport-ws");
    HttpServer server = vertx.createHttpServer(httpServerOptions);
    server.requestHandler(router).listen(port);
  }

  private static boolean isGraphiQLEnabled() {
    return Boolean.parseBoolean(System.getenv().getOrDefault("ENABLE_GRAPHIQL", "false"));
  }

  private static Map<String, KafkaProducer<String, String>> buildKafkaProducers(Vertx vertx,
                                                                                List<KafkaConnectionConfig> connections) {
    Map<String, KafkaProducer<String, String>> producers = new HashMap<>();
    if (connections == null || connections.isEmpty()) {
      return producers;
    }
    for (KafkaConnectionConfig connection : connections) {
      if (connection == null || connection.getName() == null || connection.getName().isBlank()) {
        continue;
      }
      Map<String, String> config = new HashMap<>();
      config.put("bootstrap.servers", connection.getBootstrapServers());
      config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
      config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
      if (connection.getClientId() != null && !connection.getClientId().isBlank()) {
        config.put("client.id", connection.getClientId());
      }
      if (connection.getAcks() != null && !connection.getAcks().isBlank()) {
        config.put("acks", connection.getAcks());
      }
      if (connection.getLingerMs() != null) {
        config.put("linger.ms", String.valueOf(connection.getLingerMs()));
      }
      if (connection.getBatchSize() != null) {
        config.put("batch.size", String.valueOf(connection.getBatchSize()));
      }
      producers.put(connection.getName(), KafkaProducer.create(vertx, config));
    }
    return producers;
  }

  private static Map<String, KafkaEventConfig> buildKafkaEventMap(List<KafkaEventConfig> events) {
    Map<String, KafkaEventConfig> eventMap = new HashMap<>();
    if (events == null) {
      return eventMap;
    }
    for (KafkaEventConfig event : events) {
      if (event == null || event.getName() == null || event.getName().isBlank()) {
        continue;
      }
      String graphqlName = graphqlSafeName(event.getName());
      eventMap.put(graphqlName, event);
    }
    return eventMap;
  }

  private static Map<String, JsonSchema> buildJsonSchemaIndex(List<JsonSchemaConfig> schemas) {
    Map<String, JsonSchema> schemaIndex = new HashMap<>();
    if (schemas == null || schemas.isEmpty()) {
      return schemaIndex;
    }
    ObjectMapper mapper = new ObjectMapper();
    JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012);
    for (JsonSchemaConfig schema : schemas) {
      if (schema == null || schema.getName() == null || schema.getName().isBlank() || schema.getSchema() == null) {
        continue;
      }
      JsonNode schemaNode = mapper.valueToTree(schema.getSchema());
      schemaIndex.put(schema.getName(), factory.getSchema(schemaNode));
    }
    return schemaIndex;
  }

  private static Map<String, dev.henneberger.runtime.config.GraphQLOperationConfig> indexGraphQLOperations(
    List<dev.henneberger.runtime.config.GraphQLOperationConfig> operations) {
    Map<String, dev.henneberger.runtime.config.GraphQLOperationConfig> index = new HashMap<>();
    if (operations == null || operations.isEmpty()) {
      return index;
    }
    for (dev.henneberger.runtime.config.GraphQLOperationConfig op : operations) {
      if (op == null || op.getName() == null || op.getName().isBlank()) {
        continue;
      }
      index.put(op.getName(), op);
    }
    return index;
  }

  private static void registerRestGraphQLEndpoints(
    Router router,
    List<dev.henneberger.runtime.config.RestGraphQLEndpointConfig> endpoints,
    Map<String, dev.henneberger.runtime.config.GraphQLOperationConfig> operations,
    GraphQL graphQL,
    AuthManager authManager
  ) {
    if (router == null || endpoints == null || endpoints.isEmpty()) {
      return;
    }
    if (graphQL == null) {
      LOGGER.warn("RestGraphQLEndpoint configured but GraphQL is null");
      return;
    }

    ObjectMapper mapper = new ObjectMapper();
    for (dev.henneberger.runtime.config.RestGraphQLEndpointConfig endpoint : endpoints) {
      if (endpoint == null || endpoint.getPath() == null || endpoint.getPath().isBlank()) {
        continue;
      }
      String method = normalizeMethod(endpoint.getMethod());
      Route route = router.route(HttpMethod.valueOf(method), endpoint.getPath());

      boolean shouldAuth = authManager != null && authManager.isEnabled()
        && (endpoint.getRequireAuth() == null || endpoint.getRequireAuth());
      if (shouldAuth) {
        route.handler(ctx -> {
          AuthResult result = authManager.authenticateHttp(ctx);
          if (!result.isAllowed()) {
            int status = result.isMissingToken() ? 401 : 403;
            ctx.response().setStatusCode(status).end(result.getMessage());
            return;
          }
          ctx.put("auth", result.getContext());
          ctx.next();
        });
      }

      Long bodyLimit = endpoint.getMaxBodyBytes() == null ? null : endpoint.getMaxBodyBytes().longValue();
      route.handler(ctx -> {
        if (bodyLimit != null && ctx.body() != null && ctx.body().length() > bodyLimit) {
          ctx.response().setStatusCode(413).end("Payload too large");
          return;
        }
        String contentType = endpoint.getContentType();
        if (contentType != null && !contentType.isBlank()) {
          String requestContentType = ctx.request().getHeader("Content-Type");
          String expected = contentType.toLowerCase(Locale.ROOT);
          if (requestContentType == null || !requestContentType.toLowerCase(Locale.ROOT).contains(expected)) {
            ctx.response().setStatusCode(415).end("Unsupported Content-Type");
            return;
          }
        }

        Object rawBody = null;
        JsonObject bodyObj = ctx.body() == null ? null : ctx.body().asJsonObject();
        JsonArray bodyArr = bodyObj == null && ctx.body() != null ? ctx.body().asJsonArray() : null;
        if (Boolean.TRUE.equals(endpoint.getRequireJson()) && bodyObj == null && bodyArr == null) {
          ctx.response().setStatusCode(400).end("Invalid JSON payload");
          return;
        }
        if (bodyObj != null) {
          rawBody = bodyObj.getMap();
        } else if (bodyArr != null) {
          rawBody = bodyArr.getList();
        } else {
          rawBody = Map.of();
        }

        String doc = endpoint.getDocument();
        String opName = endpoint.getOperationName();
        if ((doc == null || doc.isBlank()) && endpoint.getOperationRef() != null && !endpoint.getOperationRef().isBlank()) {
          dev.henneberger.runtime.config.GraphQLOperationConfig op = operations.get(endpoint.getOperationRef());
          if (op == null) {
            ctx.response().setStatusCode(500).end("GraphQLOperation not found: " + endpoint.getOperationRef());
            return;
          }
          doc = op.getDocument();
          if (opName == null || opName.isBlank()) {
            opName = op.getOperationName();
          }
        }
        if (doc == null || doc.isBlank()) {
          ctx.response().setStatusCode(500).end("RestGraphQLEndpoint missing document or operationRef");
          return;
        }

        Map<String, Object> variables = new HashMap<>();
        if (endpoint.getVariables() != null) {
          for (dev.henneberger.runtime.config.RestGraphQLEndpointConfig.Variable var : endpoint.getVariables()) {
            if (var == null || var.getName() == null || var.getName().isBlank() || var.getSource() == null) {
              continue;
            }
            Object value = resolveRestParamValue(ctx, rawBody, var.getSource());
            variables.put(var.getName(), value);
          }
        }

        Map<String, Object> gqlContext = new HashMap<>();
        gqlContext.put("headers", ctx.request().headers());
        AuthContext authContext = ctx.get("auth");
        if (authContext != null) {
          gqlContext.put("auth", authContext);
        }

        graphql.ExecutionInput input = graphql.ExecutionInput.newExecutionInput()
          .query(doc)
          .operationName(opName)
          .variables(variables)
          .graphQLContext(gqlContext)
          .build();

        graphQL.executeAsync(input).whenComplete((result, err) -> {
          if (err != null) {
            ctx.response().setStatusCode(500).end("GraphQL execution failed");
            return;
          }
          ctx.response().putHeader("Content-Type", "application/json");
          try {
            String json = mapper.writeValueAsString(result.toSpecification());
            ctx.response().setStatusCode(200).end(json);
          } catch (Exception e) {
            ctx.response().setStatusCode(500).end("Failed to encode GraphQL result");
          }
        });
      });
    }
  }

  private static void registerOauthEndpoints(Router router, AuthManager authManager) {
    if (router == null) {
      return;
    }

    final String wellKnownPath = "/.well-known/oauth-authorization-server";
    final String wellKnownMcpPath = "/.well-known/oauth-authorization-server/mcp";
    final String protectedResourcePath = "/.well-known/oauth-protected-resource";
    final String protectedResourceMcpPath = "/.well-known/oauth-protected-resource/mcp";
    final String authorizePath = "/oauth/authorize";
    final String tokenPath = "/oauth/token";
    final String registerPath = "/oauth/register";
    final String jwksPath = "/oauth/jwks";

    router.get(wellKnownPath).handler(ctx -> respondOauthAuthorizationServerMetadata(ctx, false, tokenPath, jwksPath));
    // Some MCP clients look for an MCP-scoped suffix.
    router.get(wellKnownMcpPath).handler(ctx -> respondOauthAuthorizationServerMetadata(ctx, true, tokenPath, jwksPath));

    router.get(protectedResourcePath).handler(ctx -> {
      String issuer = resolveOauthIssuer(ctx);
      // Minimal OAuth Protected Resource Metadata (RFC 9728-ish) so clients can discover auth
      // starting from the protected MCP resource endpoint.
      Map<String, Object> body = new LinkedHashMap<>();
      body.put("resource", issuer);
      body.put("authorization_servers", List.of(issuer));
      body.put("scopes_supported", List.of("mcp"));
      body.put("bearer_methods_supported", List.of("header"));
      ctx.response().putHeader("Content-Type", "application/json");
      try {
        ctx.response().setStatusCode(200).end(JSON_MAPPER.writeValueAsString(body));
      } catch (Exception e) {
        ctx.response().setStatusCode(500).end("Failed to encode OAuth protected resource metadata");
      }
    });

    router.get(protectedResourceMcpPath).handler(ctx -> {
      String issuer = resolveOauthIssuer(ctx);
      Map<String, Object> body = new LinkedHashMap<>();
      body.put("resource", issuer + "/mcp");
      body.put("authorization_servers", List.of(issuer));
      body.put("scopes_supported", List.of("mcp"));
      body.put("bearer_methods_supported", List.of("header"));
      ctx.response().putHeader("Content-Type", "application/json");
      try {
        ctx.response().setStatusCode(200).end(JSON_MAPPER.writeValueAsString(body));
      } catch (Exception e) {
        ctx.response().setStatusCode(500).end("Failed to encode OAuth protected resource metadata");
      }
    });

    router.get(jwksPath).handler(ctx -> {
      // This prototype mints HS256 (shared secret) tokens; it does not expose a public JWKS.
      // Some clients still require jwks_uri to exist, so return an empty set.
      ctx.response().putHeader("Content-Type", "application/json");
      try {
        ctx.response().setStatusCode(200).end(JSON_MAPPER.writeValueAsString(Map.of("keys", List.of())));
      } catch (Exception e) {
        ctx.response().setStatusCode(500).end("Failed to encode JWKS");
      }
    });

    router.post(registerPath).handler(ctx -> {
      JsonObject body = ctx.body() == null ? null : ctx.body().asJsonObject();
      if (body == null) {
        LOGGER.info("OAuth DCR POST {} -> 400 (invalid JSON)", registerPath);
        respondOauthError(ctx, 400, "invalid_request", "expected JSON body");
        return;
      }
      LOGGER.info("OAuth DCR POST {} bodyKeys={}", registerPath, body.fieldNames());

      List<String> redirectUris = jsonArrayStrings(body.getJsonArray("redirect_uris"));
      if (redirectUris.isEmpty()) {
        LOGGER.info("OAuth DCR POST {} -> 400 (missing redirect_uris)", registerPath);
        respondOauthError(ctx, 400, "invalid_client_metadata", "redirect_uris is required");
        return;
      }
      String tokenEndpointAuthMethod = stringFromAny(body.getValue("token_endpoint_auth_method"));
      if (tokenEndpointAuthMethod == null || tokenEndpointAuthMethod.isBlank()) {
        tokenEndpointAuthMethod = "none";
      }
      tokenEndpointAuthMethod = tokenEndpointAuthMethod.trim();
      if (!"none".equals(tokenEndpointAuthMethod) && !"client_secret_basic".equals(tokenEndpointAuthMethod)) {
        LOGGER.info("OAuth DCR POST {} -> 400 (unsupported token_endpoint_auth_method={})", registerPath, tokenEndpointAuthMethod);
        respondOauthError(ctx, 400, "invalid_client_metadata", "unsupported token_endpoint_auth_method");
        return;
      }

      String scope = stringFromAny(body.getValue("scope"));
      if (scope == null || scope.isBlank()) {
        scope = "mcp";
      }

      long nowSec = System.currentTimeMillis() / 1000L;
      String clientId = "mcp-" + randomToken(24);
      String clientSecret = null;
      if ("client_secret_basic".equals(tokenEndpointAuthMethod)) {
        clientSecret = randomToken(32);
      }

      OAuthClient client = new OAuthClient(clientId, clientSecret, tokenEndpointAuthMethod, redirectUris, scope);
      OAUTH_CLIENTS.put(clientId, client);

      Map<String, Object> out = new LinkedHashMap<>();
      out.put("client_id", clientId);
      out.put("client_id_issued_at", nowSec);
      out.put("redirect_uris", redirectUris);
      out.put("token_endpoint_auth_method", tokenEndpointAuthMethod);
      out.put("grant_types", List.of("authorization_code", "refresh_token"));
      out.put("response_types", List.of("code"));
      out.put("scope", scope);
      if (clientSecret != null) {
        out.put("client_secret", clientSecret);
        out.put("client_secret_expires_at", 0);
      }

      ctx.response().putHeader("Content-Type", "application/json");
      try {
        LOGGER.info("OAuth DCR POST {} -> 201 (client_id={})", registerPath, clientId);
        ctx.response().setStatusCode(201).end(JSON_MAPPER.writeValueAsString(out));
      } catch (Exception e) {
        LOGGER.info("OAuth DCR POST {} -> 500 (encode error)", registerPath);
        respondOauthError(ctx, 500, "server_error", "failed to encode registration response");
      }
    });

    router.get(authorizePath).handler(ctx -> {
      String responseType = ctx.request().getParam("response_type");
      String clientId = ctx.request().getParam("client_id");
      String redirectUri = ctx.request().getParam("redirect_uri");
      String state = ctx.request().getParam("state");
      String scope = ctx.request().getParam("scope");
      String codeChallenge = ctx.request().getParam("code_challenge");
      String codeChallengeMethod = ctx.request().getParam("code_challenge_method");

      try {
        LOGGER.info("OAuth authorize {} queryKeys={}", authorizePath, ctx.request().params() == null ? List.of() : ctx.request().params().names());
      } catch (Exception ignored) {
      }

      if (!"code".equals(responseType)) {
        ctx.response().setStatusCode(400).end("unsupported response_type");
        return;
      }
      if (clientId == null || clientId.isBlank()) {
        ctx.response().setStatusCode(400).end("missing client_id");
        return;
      }
      OAuthClient client = OAUTH_CLIENTS.get(clientId);
      if (client == null) {
        ctx.response().setStatusCode(400).end("unknown client_id");
        return;
      }
      if (redirectUri == null || redirectUri.isBlank() || !client.isRedirectAllowed(redirectUri)) {
        ctx.response().setStatusCode(400).end("invalid redirect_uri");
        return;
      }
      if (codeChallenge == null || codeChallenge.isBlank()) {
        ctx.response().setStatusCode(400).end("missing code_challenge");
        return;
      }
      if (codeChallengeMethod == null || codeChallengeMethod.isBlank()) {
        codeChallengeMethod = "S256";
      }
      if (!"S256".equalsIgnoreCase(codeChallengeMethod)) {
        ctx.response().setStatusCode(400).end("unsupported code_challenge_method");
        return;
      }

      String approved = ctx.request().getParam("approve");
      if (!"true".equalsIgnoreCase(approved)) {
        String finalScope = (scope == null || scope.isBlank()) ? client.defaultScope : scope;
        ctx.response().putHeader("Content-Type", "text/html; charset=utf-8");
        ctx.response().setStatusCode(200).end(
          "<!doctype html><html><body style=\"font-family: sans-serif; max-width: 720px; margin: 40px auto;\">" +
            "<h2>Authorize MCP Client</h2>" +
            "<p><b>client_id:</b> " + escapeHtml(clientId) + "</p>" +
            "<p><b>scope:</b> " + escapeHtml(finalScope) + "</p>" +
            "<form method=\"GET\" action=\"" + escapeHtml(authorizePath) + "\">" +
            "<input type=\"hidden\" name=\"approve\" value=\"true\"/>" +
            "<input type=\"hidden\" name=\"response_type\" value=\"" + escapeHtml(responseType) + "\"/>" +
            "<input type=\"hidden\" name=\"client_id\" value=\"" + escapeHtml(clientId) + "\"/>" +
            "<input type=\"hidden\" name=\"redirect_uri\" value=\"" + escapeHtml(redirectUri) + "\"/>" +
            (state == null ? "" : "<input type=\"hidden\" name=\"state\" value=\"" + escapeHtml(state) + "\"/>") +
            "<input type=\"hidden\" name=\"scope\" value=\"" + escapeHtml(finalScope) + "\"/>" +
            "<input type=\"hidden\" name=\"code_challenge\" value=\"" + escapeHtml(codeChallenge) + "\"/>" +
            "<input type=\"hidden\" name=\"code_challenge_method\" value=\"" + escapeHtml(codeChallengeMethod) + "\"/>" +
            "<button type=\"submit\">Approve</button>" +
            "</form>" +
            "</body></html>"
        );
        return;
      }

      String code = "c-" + randomToken(24);
      long expMs = System.currentTimeMillis() + 5 * 60 * 1000L;
      String finalScope = (scope == null || scope.isBlank()) ? client.defaultScope : scope;
      OAUTH_CODES.put(code, new OAuthAuthCode(code, clientId, redirectUri, codeChallenge, finalScope, expMs));

      String location = redirectUri
        + (redirectUri.contains("?") ? "&" : "?")
        + "code=" + urlEncode(code)
        + (state == null ? "" : "&state=" + urlEncode(state));
      ctx.response().setStatusCode(302).putHeader("Location", location).end();
    });

    router.post(tokenPath).handler(ctx -> {
      // OAuth token requests are typically form-encoded, but we accept JSON for convenience.
      String grantType = firstNonBlank(ctx.request().getFormAttribute("grant_type"), jsonBodyString(ctx, "grant_type"));
      if (grantType == null || grantType.isBlank()) {
        respondOauthError(ctx, 400, "invalid_request", "missing grant_type");
        return;
      }

      if (authManager == null || !authManager.isEnabled()) {
        respondOauthError(ctx, 501, "server_error", "auth is not enabled on this server");
        return;
      }
      byte[] sharedSecret = authManager.getJwtSharedSecretBytes();
      if (sharedSecret == null || sharedSecret.length == 0) {
        respondOauthError(ctx, 500, "server_error", "JWT sharedSecret is not configured (HS256 required for prototype)");
        return;
      }

      String issuer = resolveOauthIssuer(ctx);
      long ttlSeconds = resolveLongEnv("OAUTH_TOKEN_TTL_SECONDS", 3600L);
      long nowMs = System.currentTimeMillis();
      Date iat = new Date(nowMs);
      Date exp = new Date(nowMs + Math.max(1L, ttlSeconds) * 1000L);

      try {
        if ("authorization_code".equals(grantType)) {
          String code = firstNonBlank(ctx.request().getFormAttribute("code"), jsonBodyString(ctx, "code"));
          String redirectUri = firstNonBlank(ctx.request().getFormAttribute("redirect_uri"), jsonBodyString(ctx, "redirect_uri"));
          String codeVerifier = firstNonBlank(ctx.request().getFormAttribute("code_verifier"), jsonBodyString(ctx, "code_verifier"));
          if (code == null || code.isBlank()) {
            respondOauthError(ctx, 400, "invalid_request", "missing code");
            return;
          }
          OAuthAuthCode authCode = OAUTH_CODES.remove(code);
          if (authCode == null || authCode.isExpired()) {
            respondOauthError(ctx, 400, "invalid_grant", "invalid or expired code");
            return;
          }
          OAuthClient client = OAUTH_CLIENTS.get(authCode.clientId);
          if (client == null) {
            respondOauthError(ctx, 400, "invalid_grant", "unknown client");
            return;
          }
          if (redirectUri == null || !authCode.redirectUri.equals(redirectUri)) {
            respondOauthError(ctx, 400, "invalid_grant", "redirect_uri mismatch");
            return;
          }
          if (!verifyPkceS256(codeVerifier, authCode.codeChallenge)) {
            respondOauthError(ctx, 400, "invalid_grant", "pkce verification failed");
            return;
          }
          if ("client_secret_basic".equals(client.tokenEndpointAuthMethod)) {
            if (!verifyBasicClientSecret(ctx.request().getHeader("Authorization"), client.clientId, client.clientSecret)) {
              ctx.response().putHeader("WWW-Authenticate", "Basic realm=\"oauth\"");
              respondOauthError(ctx, 401, "invalid_client", "invalid client authentication");
              return;
            }
          } else {
            // Public client: allow token exchange without secret.
          }

          String refresh = "r-" + randomToken(32);
          OAUTH_REFRESH.put(refresh, new OAuthRefreshToken(refresh, client.clientId, authCode.scope,
            System.currentTimeMillis() + 7L * 24L * 60L * 60L * 1000L));

          JWTClaimsSet claims = new JWTClaimsSet.Builder()
            .subject(client.clientId)
            .issuer(issuer)
            .issueTime(iat)
            .expirationTime(exp)
            .claim("scope", authCode.scope)
            .build();
          SignedJWT jwt = new SignedJWT(new JWSHeader(JWSAlgorithm.HS256), claims);
          jwt.sign(new MACSigner(sharedSecret));

          Map<String, Object> ok = new LinkedHashMap<>();
          ok.put("access_token", jwt.serialize());
          ok.put("token_type", "Bearer");
          ok.put("expires_in", (int) Math.max(1L, ttlSeconds));
          ok.put("refresh_token", refresh);
          ctx.response().putHeader("Content-Type", "application/json");
          ctx.response().putHeader("Cache-Control", "no-store");
          ctx.response().putHeader("Pragma", "no-cache");
          ctx.response().setStatusCode(200).end(JSON_MAPPER.writeValueAsString(ok));
          return;
        }

        if ("refresh_token".equals(grantType)) {
          String refresh = firstNonBlank(ctx.request().getFormAttribute("refresh_token"), jsonBodyString(ctx, "refresh_token"));
          String clientIdParam = firstNonBlank(ctx.request().getFormAttribute("client_id"), jsonBodyString(ctx, "client_id"));
          if (refresh == null || refresh.isBlank()) {
            respondOauthError(ctx, 400, "invalid_request", "missing refresh_token");
            return;
          }
          OAuthRefreshToken rt = OAUTH_REFRESH.get(refresh);
          if (rt == null || rt.isExpired()) {
            respondOauthError(ctx, 400, "invalid_grant", "invalid or expired refresh_token");
            return;
          }
          OAuthClient client = OAUTH_CLIENTS.get(rt.clientId);
          if (client == null) {
            respondOauthError(ctx, 400, "invalid_grant", "unknown client");
            return;
          }
          if ("client_secret_basic".equals(client.tokenEndpointAuthMethod)) {
            if (!verifyBasicClientSecret(ctx.request().getHeader("Authorization"), client.clientId, client.clientSecret)) {
              ctx.response().putHeader("WWW-Authenticate", "Basic realm=\"oauth\"");
              respondOauthError(ctx, 401, "invalid_client", "invalid client authentication");
              return;
            }
          } else {
            if (clientIdParam == null || !client.clientId.equals(clientIdParam)) {
              respondOauthError(ctx, 401, "invalid_client", "missing or invalid client_id");
              return;
            }
          }

          JWTClaimsSet claims = new JWTClaimsSet.Builder()
            .subject(client.clientId)
            .issuer(issuer)
            .issueTime(iat)
            .expirationTime(exp)
            .claim("scope", rt.scope)
            .build();
          SignedJWT jwt = new SignedJWT(new JWSHeader(JWSAlgorithm.HS256), claims);
          jwt.sign(new MACSigner(sharedSecret));

          Map<String, Object> ok = new LinkedHashMap<>();
          ok.put("access_token", jwt.serialize());
          ok.put("token_type", "Bearer");
          ok.put("expires_in", (int) Math.max(1L, ttlSeconds));
          ctx.response().putHeader("Content-Type", "application/json");
          ctx.response().putHeader("Cache-Control", "no-store");
          ctx.response().putHeader("Pragma", "no-cache");
          ctx.response().setStatusCode(200).end(JSON_MAPPER.writeValueAsString(ok));
          return;
        }

        if ("client_credentials".equals(grantType)) {
          String scope = firstNonBlank(ctx.request().getFormAttribute("scope"), jsonBodyString(ctx, "scope"));
          String authz = ctx.request().getHeader("Authorization");
          if (authz == null || !authz.regionMatches(true, 0, "Basic ", 0, 6)) {
            ctx.response().putHeader("WWW-Authenticate", "Basic realm=\"oauth\"");
            respondOauthError(ctx, 401, "invalid_client", "missing client authentication");
            return;
          }
          AuthResult auth = authManager.authenticateHttp(ctx);
          if (!auth.isAllowed() || auth.getContext() == null) {
            ctx.response().putHeader("WWW-Authenticate", "Basic realm=\"oauth\"");
            respondOauthError(ctx, 401, "invalid_client", "invalid client credentials");
            return;
          }
          String clientId = auth.getContext().getSubject();
          String outScope = (scope == null || scope.isBlank()) ? "mcp" : scope;

          JWTClaimsSet claims = new JWTClaimsSet.Builder()
            .subject(clientId)
            .issuer(issuer)
            .issueTime(iat)
            .expirationTime(exp)
            .claim("scope", outScope)
            .build();
          SignedJWT jwt = new SignedJWT(new JWSHeader(JWSAlgorithm.HS256), claims);
          jwt.sign(new MACSigner(sharedSecret));

          Map<String, Object> ok = new LinkedHashMap<>();
          ok.put("access_token", jwt.serialize());
          ok.put("token_type", "Bearer");
          ok.put("expires_in", (int) Math.max(1L, ttlSeconds));
          ctx.response().putHeader("Content-Type", "application/json");
          ctx.response().putHeader("Cache-Control", "no-store");
          ctx.response().putHeader("Pragma", "no-cache");
          ctx.response().setStatusCode(200).end(JSON_MAPPER.writeValueAsString(ok));
          return;
        }

        respondOauthError(ctx, 400, "unsupported_grant_type", "unsupported grant_type");
      } catch (Exception e) {
        LOGGER.error("Failed to mint OAuth token", e);
        respondOauthError(ctx, 500, "server_error", "failed to mint token");
      }
    });
  }

  private static void respondOauthAuthorizationServerMetadata(io.vertx.ext.web.RoutingContext ctx,
                                                              boolean mcpScoped,
                                                              String tokenPath,
                                                              String jwksPath) {
    String issuer = resolveOauthIssuer(ctx);
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("issuer", issuer);
    // Even though this prototype only supports client_credentials, some OAuth clients
    // will look for authorization_endpoint in metadata.
    body.put("authorization_endpoint", issuer + "/oauth/authorize");
    body.put("token_endpoint", issuer + (tokenPath == null ? "/oauth/token" : tokenPath));
    body.put("jwks_uri", issuer + (jwksPath == null ? "/oauth/jwks" : jwksPath));
    if (mcpScoped) {
      body.put("registration_endpoint", issuer + "/oauth/register");
    }
    body.put("grant_types_supported", List.of("authorization_code", "refresh_token", "client_credentials"));
    body.put("response_types_supported", List.of("code"));
    body.put("code_challenge_methods_supported", List.of("S256"));
    body.put("token_endpoint_auth_methods_supported", List.of("none", "client_secret_basic"));
    body.put("scopes_supported", List.of("mcp"));
    body.put("mcp_scoped", mcpScoped);
    ctx.response().putHeader("Content-Type", "application/json");
    try {
      ctx.response().setStatusCode(200).end(JSON_MAPPER.writeValueAsString(body));
    } catch (Exception e) {
      ctx.response().setStatusCode(500).end("Failed to encode OAuth metadata");
    }
  }

  private static void attachOauthWwwAuthenticate(io.vertx.ext.web.RoutingContext ctx, String realm) {
    attachOauthWwwAuthenticate(ctx, realm, null);
  }

  private static void attachOauthWwwAuthenticate(io.vertx.ext.web.RoutingContext ctx, String realm, String error) {
    if (ctx == null || ctx.response() == null) {
      return;
    }
    String issuer = resolveOauthIssuer(ctx);
    String path = ctx.request() == null ? null : ctx.request().path();
    boolean mcp = path != null && (path.equals("/mcp") || path.startsWith("/mcp/"));
    String rm = issuer + (mcp ? "/.well-known/oauth-protected-resource/mcp" : "/.well-known/oauth-protected-resource");
    String r = realm == null || realm.isBlank() ? "mcp" : realm.trim();

    StringBuilder value = new StringBuilder();
    value.append("Bearer realm=\"").append(r.replace("\"", "")).append("\"");
    value.append(", scope=\"mcp\"");
    value.append(", resource_metadata=\"").append(rm.replace("\"", "")).append("\"");
    if (error != null && !error.isBlank()) {
      value.append(", error=\"").append(error.replace("\"", "")).append("\"");
    }
    ctx.response().putHeader("WWW-Authenticate", value.toString());
    LOGGER.info("WWW-Authenticate {}", value);
  }

  private static String resolveOauthIssuer(io.vertx.ext.web.RoutingContext ctx) {
    String override = System.getenv("OAUTH_ISSUER");
    if (override != null && !override.isBlank()) {
      return override.trim().replaceAll("/+$", "");
    }
    if (ctx == null || ctx.request() == null) {
      return "http://localhost:8080";
    }
    String scheme = ctx.request().scheme();
    String host = ctx.request().getHeader("Host");
    if (scheme == null || scheme.isBlank()) {
      scheme = "http";
    }
    if (host == null || host.isBlank()) {
      host = "localhost";
    }
    return scheme + "://" + host.replaceAll("/+$", "");
  }

  private static String jsonBodyString(io.vertx.ext.web.RoutingContext ctx, String key) {
    if (ctx == null || key == null) {
      return null;
    }
    try {
      JsonObject body = ctx.body() == null ? null : ctx.body().asJsonObject();
      if (body == null) {
        return null;
      }
      Object v = body.getValue(key);
      return v == null ? null : String.valueOf(v);
    } catch (Exception ignored) {
      // Non-JSON bodies (e.g. application/x-www-form-urlencoded) will throw if we try to decode as JSON.
      return null;
    }
  }

  private static String firstNonBlank(String a, String b) {
    if (a != null && !a.isBlank()) {
      return a;
    }
    if (b != null && !b.isBlank()) {
      return b;
    }
    return a != null ? a : b;
  }

  private static long resolveLongEnv(String name, long def) {
    String raw = name == null ? null : System.getenv(name);
    if (raw == null || raw.isBlank()) {
      return def;
    }
    try {
      return Long.parseLong(raw.trim());
    } catch (Exception ignored) {
      return def;
    }
  }

  private static void respondOauthError(io.vertx.ext.web.RoutingContext ctx, int status, String error, String desc) {
    if (ctx == null) {
      return;
    }
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("error", error == null ? "server_error" : error);
    if (desc != null && !desc.isBlank()) {
      body.put("error_description", desc);
    }
    ctx.response().putHeader("Content-Type", "application/json");
    ctx.response().putHeader("Cache-Control", "no-store");
    ctx.response().putHeader("Pragma", "no-cache");
    try {
      ctx.response().setStatusCode(status).end(JSON_MAPPER.writeValueAsString(body));
    } catch (Exception e) {
      ctx.response().setStatusCode(status).end("{\"error\":\"server_error\"}");
    }
  }

  private static boolean verifyBasicClientSecret(String headerValue, String clientId, String clientSecret) {
    if (headerValue == null || !headerValue.regionMatches(true, 0, "Basic ", 0, 6)) {
      return false;
    }
    String decoded = decodeBasicToken(headerValue);
    if (decoded == null) {
      return false;
    }
    int idx = decoded.indexOf(':');
    if (idx <= 0) {
      return false;
    }
    String u = decoded.substring(0, idx);
    String p = decoded.substring(idx + 1);
    return Objects.equals(u, clientId) && Objects.equals(p, clientSecret);
  }

  private static String decodeBasicToken(String headerValue) {
    if (headerValue == null) {
      return null;
    }
    String trimmed = headerValue.trim();
    if (!trimmed.regionMatches(true, 0, "Basic ", 0, 6)) {
      return null;
    }
    String encoded = trimmed.substring(6).trim();
    if (encoded.isEmpty()) {
      return null;
    }
    try {
      byte[] decoded = java.util.Base64.getDecoder().decode(encoded);
      return new String(decoded, StandardCharsets.UTF_8);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  private static boolean verifyPkceS256(String codeVerifier, String expectedChallenge) {
    if (codeVerifier == null || codeVerifier.isBlank() || expectedChallenge == null || expectedChallenge.isBlank()) {
      return false;
    }
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      byte[] digest = md.digest(codeVerifier.getBytes(StandardCharsets.US_ASCII));
      String actual = Base64.getUrlEncoder().withoutPadding().encodeToString(digest);
      return Objects.equals(actual, expectedChallenge);
    } catch (Exception e) {
      return false;
    }
  }

  private static List<String> jsonArrayStrings(JsonArray arr) {
    if (arr == null || arr.isEmpty()) {
      return Collections.emptyList();
    }
    List<String> out = new ArrayList<>();
    for (int i = 0; i < arr.size(); i++) {
      Object v = arr.getValue(i);
      if (v != null) {
        String s = String.valueOf(v).trim();
        if (!s.isBlank()) {
          out.add(s);
        }
      }
    }
    return out;
  }

  private static String randomToken(int nBytes) {
    byte[] b = new byte[Math.max(8, nBytes)];
    OAUTH_RAND.nextBytes(b);
    return Base64.getUrlEncoder().withoutPadding().encodeToString(b);
  }

  private static String escapeHtml(String s) {
    if (s == null) {
      return "";
    }
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;");
  }

  private static String stringFromAny(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof String) {
      return (String) value;
    }
    return String.valueOf(value);
  }

  private static final class OAuthClient {
    private final String clientId;
    private final String clientSecret; // null for public clients
    private final String tokenEndpointAuthMethod; // "none" | "client_secret_basic"
    private final List<String> redirectUris;
    private final String defaultScope;

    private OAuthClient(String clientId,
                        String clientSecret,
                        String tokenEndpointAuthMethod,
                        List<String> redirectUris,
                        String defaultScope) {
      this.clientId = clientId;
      this.clientSecret = clientSecret;
      this.tokenEndpointAuthMethod = tokenEndpointAuthMethod;
      this.redirectUris = redirectUris == null ? Collections.emptyList() : List.copyOf(redirectUris);
      this.defaultScope = defaultScope == null ? "mcp" : defaultScope;
    }

    private boolean isRedirectAllowed(String uri) {
      if (uri == null) {
        return false;
      }
      for (String allowed : redirectUris) {
        if (uri.equals(allowed)) {
          return true;
        }
      }
      return false;
    }
  }

  private static final class OAuthAuthCode {
    private final String code;
    private final String clientId;
    private final String redirectUri;
    private final String codeChallenge;
    private final String scope;
    private final long expMs;

    private OAuthAuthCode(String code,
                          String clientId,
                          String redirectUri,
                          String codeChallenge,
                          String scope,
                          long expMs) {
      this.code = code;
      this.clientId = clientId;
      this.redirectUri = redirectUri;
      this.codeChallenge = codeChallenge;
      this.scope = scope == null ? "mcp" : scope;
      this.expMs = expMs;
    }

    private boolean isExpired() {
      return System.currentTimeMillis() > expMs;
    }
  }

  private static final class OAuthRefreshToken {
    private final String token;
    private final String clientId;
    private final String scope;
    private final long expMs;

    private OAuthRefreshToken(String token, String clientId, String scope, long expMs) {
      this.token = token;
      this.clientId = clientId;
      this.scope = scope == null ? "mcp" : scope;
      this.expMs = expMs;
    }

    private boolean isExpired() {
      return System.currentTimeMillis() > expMs;
    }
  }

  private static void registerMcpServers(
    Router router,
    List<McpServerConfig> servers,
    Map<String, dev.henneberger.runtime.config.GraphQLOperationConfig> operations,
    GraphQL graphQL,
    AuthManager authManager
  ) {
    if (router == null || servers == null || servers.isEmpty()) {
      return;
    }
    if (graphQL == null) {
      LOGGER.warn("McpServer configured but GraphQL is null");
      return;
    }

    ObjectMapper mapper = new ObjectMapper();
    for (McpServerConfig server : servers) {
      if (server == null) {
        continue;
      }
      String path = server.getPath();
      if (path == null || path.isBlank()) {
        path = "/mcp";
      }

      Route postRoute = router.route(HttpMethod.POST, path);
      Route getRoute = router.route(HttpMethod.GET, path);

      boolean shouldAuth = authManager != null && authManager.isEnabled()
        && (server.getRequireAuth() == null || server.getRequireAuth());
      if (shouldAuth) {
        postRoute.handler(ctx -> {
          AuthResult result = authManager.authenticateHttp(ctx);
          if (!result.isAllowed()) {
            if (result.isMissingToken()) {
              attachOauthWwwAuthenticate(ctx, "mcp");
              ctx.response().setStatusCode(401).end(result.getMessage());
              return;
            }
            attachOauthWwwAuthenticate(ctx, "mcp", "invalid_token");
            ctx.response().setStatusCode(401).end(result.getMessage());
            return;
          }
          ctx.put("auth", result.getContext());
          ctx.next();
        });
        getRoute.handler(ctx -> {
          AuthResult result = authManager.authenticateHttp(ctx);
          if (!result.isAllowed()) {
            if (result.isMissingToken()) {
              attachOauthWwwAuthenticate(ctx, "mcp");
              ctx.response().setStatusCode(401).end(result.getMessage());
              return;
            }
            attachOauthWwwAuthenticate(ctx, "mcp", "invalid_token");
            ctx.response().setStatusCode(401).end(result.getMessage());
            return;
          }
          ctx.put("auth", result.getContext());
          ctx.next();
        });
      }

      // Basic compatibility response for clients that try GET first (SSE mode).
      getRoute.handler(ctx -> {
        ctx.response().putHeader("Content-Type", "application/json");
        Map<String, Object> err = jsonRpcError(null, -32000, "MCP endpoint expects POST JSON-RPC requests");
        try {
          ctx.response().setStatusCode(200).end(mapper.writeValueAsString(err));
        } catch (Exception e) {
          ctx.response().setStatusCode(500).end("Failed to encode response");
        }
      });

      postRoute.handler(ctx -> {
        JsonObject body = ctx.body() == null ? null : ctx.body().asJsonObject();
        if (body == null) {
          ctx.response().setStatusCode(400).end("Invalid JSON-RPC payload");
          return;
        }

        Object id = body.getValue("id");
        String method = body.getString("method");
        JsonObject params = body.getJsonObject("params");

        // Notifications: no id, no response.
        if (id == null) {
          ctx.response().setStatusCode(204).end();
          return;
        }

        if (method == null || method.isBlank()) {
          respondJson(ctx, mapper, jsonRpcError(id, -32600, "Missing method"));
          return;
        }

        if ("initialize".equals(method)) {
          String protocolVersion = params == null ? null : params.getString("protocolVersion");
          if (protocolVersion == null || protocolVersion.isBlank()) {
            protocolVersion = "2024-11-05";
          }
          Map<String, Object> result = new LinkedHashMap<>();
          result.put("protocolVersion", protocolVersion);
          result.put("capabilities", Map.of("tools", Map.of()));
          result.put("serverInfo", Map.of(
            "name", server.getName() == null ? "vertx-graphql-mcp" : server.getName(),
            "version", "0.1.0"
          ));
          // A lightweight "session" header for clients that care, even though we accept stateless calls.
          ctx.response().putHeader("mcp-session-id", UUID.randomUUID().toString());
          respondJson(ctx, mapper, jsonRpcResult(id, result));
          return;
        }

        if ("tools/list".equals(method)) {
          List<Map<String, Object>> tools = new ArrayList<>();
          Map<String, dev.henneberger.runtime.config.GraphQLOperationConfig> opByTool =
            indexToolsForServer(server, operations);
          for (Map.Entry<String, dev.henneberger.runtime.config.GraphQLOperationConfig> entry : opByTool.entrySet()) {
            String toolName = entry.getKey();
            dev.henneberger.runtime.config.GraphQLOperationConfig op = entry.getValue();
            Map<String, Object> tool = new LinkedHashMap<>();
            tool.put("name", toolName);
            if (op.getDescription() != null && !op.getDescription().isBlank()) {
              tool.put("description", op.getDescription());
            }
            tool.put("inputSchema", buildToolInputSchema(op.getDocument(), op.getOperationName()));
            tools.add(tool);
          }
          respondJson(ctx, mapper, jsonRpcResult(id, Map.of("tools", tools)));
          return;
        }

        if ("tools/call".equals(method)) {
          if (params == null) {
            respondJson(ctx, mapper, jsonRpcError(id, -32602, "Missing params"));
            return;
          }
          String toolName = params.getString("name");
          JsonObject argsObj = params.getJsonObject("arguments");
          Map<String, Object> args = argsObj == null ? Map.of() : argsObj.getMap();
          if (toolName == null || toolName.isBlank()) {
            respondJson(ctx, mapper, jsonRpcError(id, -32602, "Missing params.name"));
            return;
          }
          Map<String, dev.henneberger.runtime.config.GraphQLOperationConfig> opByTool =
            indexToolsForServer(server, operations);
          dev.henneberger.runtime.config.GraphQLOperationConfig op = opByTool.get(toolName);
          if (op == null) {
            respondJson(ctx, mapper, jsonRpcError(id, -32602, "Unknown tool: " + toolName));
            return;
          }
          if (op.getDocument() == null || op.getDocument().isBlank()) {
            respondJson(ctx, mapper, jsonRpcError(id, -32603, "Tool has no document: " + toolName));
            return;
          }

          Map<String, Object> gqlContext = new HashMap<>();
          gqlContext.put("headers", ctx.request().headers());
          AuthContext authContext = ctx.get("auth");
          if (authContext != null) {
            gqlContext.put("auth", authContext);
          }

          graphql.ExecutionInput input = graphql.ExecutionInput.newExecutionInput()
            .query(op.getDocument())
            .operationName(op.getOperationName())
            .variables(args)
            .graphQLContext(gqlContext)
            .build();

          graphQL.executeAsync(input).whenComplete((result, err) -> {
            if (err != null) {
              respondJson(ctx, mapper, jsonRpcError(id, -32603, "GraphQL execution failed"));
              return;
            }
            Map<String, Object> spec = result == null ? Map.of() : result.toSpecification();
            boolean isError = result != null && result.getErrors() != null && !result.getErrors().isEmpty();
            String text;
            try {
              text = mapper.writeValueAsString(spec);
            } catch (Exception e) {
              text = String.valueOf(spec);
            }
            Map<String, Object> callResult = new LinkedHashMap<>();
            callResult.put("content", List.of(Map.of("type", "text", "text", text)));
            if (isError) {
              callResult.put("isError", true);
            }
            respondJson(ctx, mapper, jsonRpcResult(id, callResult));
          });
          return;
        }

        // MCP "notification" methods sometimes show up; respond with method not found for request/response.
        respondJson(ctx, mapper, jsonRpcError(id, -32601, "Method not found: " + method));
      });
    }
  }

  private static Map<String, dev.henneberger.runtime.config.GraphQLOperationConfig> indexToolsForServer(
    McpServerConfig server,
    Map<String, dev.henneberger.runtime.config.GraphQLOperationConfig> operations
  ) {
    Map<String, dev.henneberger.runtime.config.GraphQLOperationConfig> out = new LinkedHashMap<>();
    if (operations == null || operations.isEmpty()) {
      return out;
    }
    Set<String> allow = null;
    if (server != null && server.getOperationRefs() != null && !server.getOperationRefs().isEmpty()) {
      allow = new HashSet<>();
      for (String ref : server.getOperationRefs()) {
        if (ref != null && !ref.isBlank()) {
          allow.add(ref);
        }
      }
    }
    McpServerConfig.ToolNameStrategy strategy =
      server == null || server.getToolNameStrategy() == null
        ? McpServerConfig.ToolNameStrategy.RESOURCE_NAME
        : server.getToolNameStrategy();

    for (dev.henneberger.runtime.config.GraphQLOperationConfig op : operations.values()) {
      if (op == null || op.getName() == null || op.getName().isBlank()) {
        continue;
      }
      if (allow != null && !allow.contains(op.getName())) {
        continue;
      }
      String toolName;
      if (strategy == McpServerConfig.ToolNameStrategy.OPERATION_NAME
        && op.getOperationName() != null && !op.getOperationName().isBlank()) {
        toolName = toToolName(op.getOperationName());
      } else {
        toolName = toToolName(op.getName());
      }
      // First one wins on collisions.
      out.putIfAbsent(toolName, op);
    }
    return out;
  }

  private static String toToolName(String s) {
    if (s == null) {
      return "";
    }
    // Prefer snake_case since most MCP examples use it.
    String cleaned = s.trim()
      .replace('-', '_')
      .replace(' ', '_');
    cleaned = cleaned.replaceAll("[^a-zA-Z0-9_]", "_");
    cleaned = cleaned.replaceAll("_+", "_");
    cleaned = cleaned.replaceAll("^_+|_+$", "");
    if (cleaned.isBlank()) {
      return "tool";
    }
    // Normalize to lower_snake_case for stability.
    return cleaned.toLowerCase(Locale.ROOT);
  }

  private static Map<String, Object> buildToolInputSchema(String document, String operationName) {
    Map<String, Object> schema = new LinkedHashMap<>();
    schema.put("type", "object");
    schema.put("additionalProperties", false);

    Map<String, Object> props = new LinkedHashMap<>();
    List<String> required = new ArrayList<>();

    if (document != null && !document.isBlank()) {
      try {
        graphql.parser.Parser parser = new graphql.parser.Parser();
        graphql.language.Document doc = parser.parseDocument(document);
        OperationDefinition op = null;
        for (graphql.language.Definition<?> def : doc.getDefinitions()) {
          if (!(def instanceof OperationDefinition)) {
            continue;
          }
          OperationDefinition od = (OperationDefinition) def;
          if (operationName == null || operationName.isBlank()) {
            op = od;
            break;
          }
          if (od.getName() != null && operationName.equals(od.getName())) {
            op = od;
            break;
          }
        }
        if (op != null && op.getVariableDefinitions() != null) {
          for (VariableDefinition vd : op.getVariableDefinitions()) {
            if (vd == null || vd.getName() == null) {
              continue;
            }
            String varName = vd.getName();
            Type<?> gqlType = vd.getType();
            boolean nonNull = (gqlType instanceof NonNullType);
            Type<?> unwrapped = nonNull ? ((NonNullType) gqlType).getType() : gqlType;
            props.put(varName, jsonSchemaForType(unwrapped));
            if (nonNull) {
              required.add(varName);
            }
          }
        }
      } catch (Exception e) {
        // Fall back to a permissive schema; validation is best-effort.
      }
    }

    schema.put("properties", props);
    if (!required.isEmpty()) {
      schema.put("required", required);
    }
    return schema;
  }

  private static Map<String, Object> jsonSchemaForType(Type<?> gqlType) {
    if (gqlType instanceof NonNullType) {
      return jsonSchemaForType(((NonNullType) gqlType).getType());
    }
    if (gqlType instanceof ListType) {
      Map<String, Object> s = new LinkedHashMap<>();
      s.put("type", "array");
      s.put("items", jsonSchemaForType(((ListType) gqlType).getType()));
      return s;
    }
    if (gqlType instanceof TypeName) {
      String name = ((TypeName) gqlType).getName();
      if (name == null) {
        return Map.of("type", "object");
      }
      switch (name) {
        case "String":
        case "ID":
          return Map.of("type", "string");
        case "Int":
          return Map.of("type", "integer");
        case "Float":
          return Map.of("type", "number");
        case "Boolean":
          return Map.of("type", "boolean");
        default:
          // Input objects and custom scalars: treat as free-form JSON object.
          return Map.of("type", "object");
      }
    }
    return Map.of("type", "object");
  }

  private static Map<String, Object> jsonRpcResult(Object id, Object result) {
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("jsonrpc", "2.0");
    out.put("id", id);
    out.put("result", result);
    return out;
  }

  private static Map<String, Object> jsonRpcError(Object id, int code, String message) {
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("jsonrpc", "2.0");
    out.put("id", id);
    out.put("error", Map.of("code", code, "message", message));
    return out;
  }

  private static void respondJson(io.vertx.ext.web.RoutingContext ctx, ObjectMapper mapper, Map<String, Object> body) {
    ctx.response().putHeader("Content-Type", "application/json");
    try {
      ctx.response().setStatusCode(200).end(mapper.writeValueAsString(body));
    } catch (Exception e) {
      ctx.response().setStatusCode(500).end("Failed to encode response");
    }
  }

  // Helper routes for an external MCP gateway (e.g. Node/Python) that wants to expose tools
  // derived from GraphQLOperations while using a proper MCP transport implementation.
  //
  // - GET  /mcp/tools   -> list tools + operationRef mapping
  // - POST /mcp/execute -> execute a GraphQLOperation by name
  private static void registerMcpGatewayBackend(
    Router router,
    Map<String, dev.henneberger.runtime.config.GraphQLOperationConfig> operations,
    GraphQL graphQL,
    AuthManager authManager
  ) {
    if (router == null || graphQL == null) {
      return;
    }

    final ObjectMapper mapper = new ObjectMapper();

    Route toolsRoute = router.route(HttpMethod.GET, "/mcp/tools");
    Route execRoute = router.route(HttpMethod.POST, "/mcp/execute");

    boolean shouldAuth = authManager != null && authManager.isEnabled();
    if (shouldAuth) {
      toolsRoute.handler(ctx -> {
        AuthResult result = authManager.authenticateHttp(ctx);
        if (!result.isAllowed()) {
          if (result.isMissingToken()) {
            attachOauthWwwAuthenticate(ctx, "mcp");
            ctx.response().setStatusCode(401).end(result.getMessage());
            return;
          }
          attachOauthWwwAuthenticate(ctx, "mcp", "invalid_token");
          ctx.response().setStatusCode(401).end(result.getMessage());
          return;
        }
        ctx.put("auth", result.getContext());
        ctx.next();
      });
      execRoute.handler(ctx -> {
        AuthResult result = authManager.authenticateHttp(ctx);
        if (!result.isAllowed()) {
          if (result.isMissingToken()) {
            attachOauthWwwAuthenticate(ctx, "mcp");
            ctx.response().setStatusCode(401).end(result.getMessage());
            return;
          }
          attachOauthWwwAuthenticate(ctx, "mcp", "invalid_token");
          ctx.response().setStatusCode(401).end(result.getMessage());
          return;
        }
        ctx.put("auth", result.getContext());
        ctx.next();
      });
    }

    toolsRoute.handler(ctx -> {
      List<Map<String, Object>> tools = new ArrayList<>();
      if (operations != null) {
        for (dev.henneberger.runtime.config.GraphQLOperationConfig op : operations.values()) {
          if (op == null || op.getName() == null || op.getName().isBlank()) {
            continue;
          }
          String toolName = toToolName(op.getName());
          Map<String, Object> tool = new LinkedHashMap<>();
          tool.put("name", toolName);
          if (op.getDescription() != null && !op.getDescription().isBlank()) {
            tool.put("description", op.getDescription());
          }
          tool.put("inputSchema", buildToolInputSchema(op.getDocument(), op.getOperationName()));
          tool.put("operationRef", op.getName());
          tools.add(tool);
        }
      }
      ctx.response().putHeader("Content-Type", "application/json");
      try {
        ctx.response().setStatusCode(200).end(mapper.writeValueAsString(Map.of("tools", tools)));
      } catch (Exception e) {
        ctx.response().setStatusCode(500).end("Failed to encode tools");
      }
    });

    execRoute.handler(ctx -> {
      JsonObject body = ctx.body() == null ? null : ctx.body().asJsonObject();
      if (body == null) {
        ctx.response().setStatusCode(400).end("Invalid JSON payload");
        return;
      }

      String operationRef = body.getString("operationRef");
      if (operationRef == null || operationRef.isBlank()) {
        // Support a few alternative keys for convenience.
        operationRef = body.getString("operation_ref");
      }
      if (operationRef == null || operationRef.isBlank()) {
        ctx.response().setStatusCode(400).end("Missing operationRef");
        return;
      }

      Object rawArgs = body.getValue("arguments");
      Map<String, Object> args = Map.of();
      if (rawArgs instanceof Map) {
        args = (Map<String, Object>) rawArgs;
      } else if (rawArgs instanceof JsonObject) {
        args = ((JsonObject) rawArgs).getMap();
      } else if (rawArgs != null) {
        ctx.response().setStatusCode(400).end("arguments must be an object");
        return;
      }

      dev.henneberger.runtime.config.GraphQLOperationConfig op =
        operations == null ? null : operations.get(operationRef);
      if (op == null) {
        ctx.response().setStatusCode(404).end("GraphQLOperation not found: " + operationRef);
        return;
      }
      if (op.getDocument() == null || op.getDocument().isBlank()) {
        ctx.response().setStatusCode(500).end("GraphQLOperation missing document: " + operationRef);
        return;
      }

      Map<String, Object> gqlContext = new HashMap<>();
      gqlContext.put("headers", ctx.request().headers());
      AuthContext authContext = ctx.get("auth");
      if (authContext != null) {
        gqlContext.put("auth", authContext);
      }

      graphql.ExecutionInput input = graphql.ExecutionInput.newExecutionInput()
        .query(op.getDocument())
        .operationName(op.getOperationName())
        .variables(args)
        .graphQLContext(gqlContext)
        .build();

      graphQL.executeAsync(input).whenComplete((result, err) -> {
        if (err != null) {
          ctx.response().setStatusCode(500).end("GraphQL execution failed");
          return;
        }
        ctx.response().putHeader("Content-Type", "application/json");
        try {
          String json = mapper.writeValueAsString(result.toSpecification());
          ctx.response().setStatusCode(200).end(json);
        } catch (Exception e) {
          ctx.response().setStatusCode(500).end("Failed to encode GraphQL result");
        }
      });
    });
  }

  private static Object resolveRestParamValue(io.vertx.ext.web.RoutingContext ctx,
                                             Object rawBody,
                                             dev.henneberger.runtime.config.RestParamSourceConfig source) {
    if (source == null || source.getKind() == null) {
      return null;
    }
    String name = source.getName();
    switch (source.getKind()) {
      case HEADER:
        return name == null ? null : ctx.request().getHeader(name);
      case QUERY:
        return name == null ? null : ctx.request().getParam(name);
      case PATH:
        return name == null ? null : ctx.pathParam(name);
      case ENV:
        return name == null ? null : System.getenv(name);
      case JWT:
        AuthContext auth = ctx.get("auth");
        if (auth == null || name == null || name.isBlank()) {
          return null;
        }
        Object claims = auth.getClaims();
        if (claims instanceof Map) {
          return ((Map<?, ?>) claims).get(name);
        }
        return null;
      case RAW_BODY:
        return rawBody;
      case BODY:
      default:
        if (rawBody == null || name == null || name.isBlank()) {
          return null;
        }
        if (!(rawBody instanceof Map)) {
          return null;
        }
        Map<String, Object> map = (Map<String, Object>) rawBody;
        String key = name.trim();
        Object direct = map.get(key);
        if (direct != null) {
          return direct;
        }
        String jsonPath = null;
        if (key.startsWith("$")) {
          jsonPath = key;
        } else if (key.contains(".") || key.contains("[")) {
          jsonPath = "$." + key;
        }
        if (jsonPath != null) {
          try {
            return com.jayway.jsonpath.JsonPath.read(map, jsonPath);
          } catch (Exception e) {
            LOGGER.warn("REST BODY jsonpath resolve failed path={} err={}", jsonPath, e.getMessage());
          }
        }
        return null;
    }
  }

  private static void registerIngestionEndpoints(Router router,
                                                 List<IngestionEndpointConfig> endpoints,
                                                 Map<String, KafkaProducer<String, String>> producers,
                                                 Map<String, JsonSchema> schemaIndex) {
    if (endpoints == null || endpoints.isEmpty()) {
      return;
    }
    ObjectMapper mapper = new ObjectMapper();
    for (IngestionEndpointConfig endpoint : endpoints) {
      if (endpoint == null || endpoint.getPath() == null || endpoint.getPath().isBlank()) {
        continue;
      }
      if (endpoint.getTopic() == null || endpoint.getTopic().isBlank()) {
        LOGGER.warn("IngestionEndpoint {} missing topic", endpoint.getName());
        continue;
      }
      String method = normalizeMethod(endpoint.getMethod());
      Route route = router.route(HttpMethod.valueOf(method), endpoint.getPath());
      Long bodyLimit = endpoint.getMaxBodyBytes() == null ? null : endpoint.getMaxBodyBytes().longValue();
      route.handler(ctx -> {
        if (bodyLimit != null && ctx.body() != null && ctx.body().length() > bodyLimit) {
          ctx.response().setStatusCode(413).end("Payload too large");
          return;
        }
        String contentType = endpoint.getContentType();
        if (contentType != null && !contentType.isBlank()) {
          String requestContentType = ctx.request().getHeader("Content-Type");
          String expected = contentType.toLowerCase(Locale.ROOT);
          if (requestContentType == null || !requestContentType.toLowerCase(Locale.ROOT).contains(expected)) {
            ctx.response().setStatusCode(415).end("Unsupported Content-Type");
            return;
          }
        }
        JsonObject payload = ctx.body() == null ? null : ctx.body().asJsonObject();
        if (Boolean.TRUE.equals(endpoint.getRequireJson()) && payload == null) {
          ctx.response().setStatusCode(400).end("Invalid JSON payload");
          return;
        }
        if (payload == null) {
          payload = new JsonObject();
        }
        if (endpoint.getAddFields() != null && !endpoint.getAddFields().isEmpty()) {
          applyAddFields(endpoint.getAddFields(), payload);
        }
        if (!validatePayload(endpoint, payload, schemaIndex, mapper)) {
          ctx.response().setStatusCode(400).end("JSON schema validation failed");
          return;
        }
        KafkaProducer<String, String> producer = producers.get(endpoint.getConnectionRef());
        if (producer == null) {
          ctx.response().setStatusCode(500).end("Kafka connection not configured");
          return;
        }
        String key = resolvePartitionKey(endpoint.getPartitionKeyField(), payload);
        JsonObject finalPayload = payload;
        KafkaProducerRecord<String, String> record =
          KafkaProducerRecord.create(endpoint.getTopic(), key, finalPayload.encode());
        producer.send(record, ar -> {
          if (ar.failed()) {
            LOGGER.error("Ingestion publish failed for endpoint={} topic={}", endpoint.getName(), endpoint.getTopic(),
              ar.cause());
            ctx.response().setStatusCode(500).end("Kafka publish failed");
          } else {
            int status = endpoint.getResponseCode() == null ? 202 : endpoint.getResponseCode();
            String responseContentType = endpoint.getResponseContentType();
            if (responseContentType == null || responseContentType.isBlank()) {
              responseContentType = "application/json";
            }
            ctx.response().putHeader("Content-Type", responseContentType);
            JsonObject responsePayload = buildResponsePayload(finalPayload, endpoint.getResponseFields());
            ctx.response().setStatusCode(status).end(responsePayload.encode());
          }
        });
      });
    }
  }

  private static String normalizeMethod(String method) {
    if (method == null || method.isBlank()) {
      return "POST";
    }
    return method.trim().toUpperCase(Locale.ROOT);
  }

  private static void applyAddFields(List<AddFieldConfig> addFields, JsonObject payload) {
    Map<String, Supplier<Object>> registry = buildAddFieldRegistry();
    for (AddFieldConfig field : addFields) {
      if (field == null || field.getName() == null || field.getName().isBlank()) {
        continue;
      }
      String type = field.getType();
      if (type == null || type.isBlank()) {
        continue;
      }
      Supplier<Object> supplier = registry.get(type);
      if (supplier == null) {
        LOGGER.warn("Unknown addField type '{}' for field={}", type, field.getName());
        continue;
      }
      payload.put(field.getName(), supplier.get());
    }
  }

  private static Map<String, Supplier<Object>> buildAddFieldRegistry() {
    Map<String, Supplier<Object>> registry = new HashMap<>();
    registry.put("uuid", () -> java.util.UUID.randomUUID().toString());
    registry.put("now", () -> java.time.OffsetDateTime.now(java.time.ZoneId.of("UTC")).toString());
    registry.put("utcNow", () -> java.time.OffsetDateTime.now(java.time.ZoneId.of("UTC")).toString());
    return registry;
  }

  private static boolean validatePayload(IngestionEndpointConfig endpoint,
                                         JsonObject payload,
                                         Map<String, JsonSchema> schemaIndex,
                                         ObjectMapper mapper) {
    String schemaRef = endpoint.getJsonSchemaRef();
    if (schemaRef == null || schemaRef.isBlank()) {
      return true;
    }
    JsonSchema schema = schemaIndex.get(schemaRef);
    if (schema == null) {
      LOGGER.error("JsonSchema {} not found for endpoint={}", schemaRef, endpoint.getName());
      return false;
    }
    JsonNode node = mapper.valueToTree(payload.getMap());
    Set<ValidationMessage> errors = schema.validate(node);
    if (errors == null || errors.isEmpty()) {
      return true;
    }
    LOGGER.warn("Json schema validation failed for endpoint={} errors={}", endpoint.getName(), errors);
    return false;
  }

  private static String resolvePartitionKey(String partitionKeyField, JsonObject payload) {
    if (partitionKeyField == null || partitionKeyField.isBlank()) {
      return null;
    }
    Object value = payload.getValue(partitionKeyField);
    if (value == null) {
      return null;
    }
    return String.valueOf(value);
  }

  private static JsonObject buildResponsePayload(JsonObject payload, List<String> responseFields) {
    if (responseFields == null || responseFields.isEmpty()) {
      return payload.copy();
    }
    JsonObject response = new JsonObject();
    boolean includeAll = responseFields.contains("*");
    if (includeAll) {
      response.mergeIn(payload);
    }
    for (String field : responseFields) {
      if (field == null || field.isBlank() || "*".equals(field)) {
        continue;
      }
      if (payload.containsKey(field)) {
        response.put(field, payload.getValue(field));
      }
    }
    return response;
  }

  private static String encodeEventPayload(Object eventPayload) {
    if (eventPayload == null) {
      return "null";
    }
    if (eventPayload instanceof JsonObject) {
      return ((JsonObject) eventPayload).encode();
    }
    if (eventPayload instanceof JsonArray) {
      return ((JsonArray) eventPayload).encode();
    }
    if (eventPayload instanceof String) {
      return (String) eventPayload;
    }
    if (eventPayload instanceof Number || eventPayload instanceof Boolean) {
      return String.valueOf(eventPayload);
    }
    return JsonObject.mapFrom(eventPayload).encode();
  }

  private static PgConnectOptions buildConnectOptions(ConnectionConfig connection) {
    PgConnectOptions options = new PgConnectOptions()
      .setHost(connection.getHost())
      .setPort(connection.getPort() == null ? 5432 : connection.getPort())
      .setDatabase(connection.getDatabase())
      .setUser(connection.getUser());

    if (Boolean.TRUE.equals(connection.getSsl())) {
      options.setSsl(true);
    }

    String password = connection.getPassword();
    if (password == null || password.isBlank()) {
      String envName = connection.getPasswordEnv();
      if (envName != null && !envName.isBlank()) {
        password = System.getenv(envName);
      }
    }
    if (password != null && !password.isBlank()) {
      options.setPassword(password);
    }

    return options;
  }

  private static GraphQL buildGraphQL(Vertx vertx,
                                      Map<String, PgPool> pools,
                                      Map<String, DuckDbConnectionConfig> duckdbConnections,
                                      Map<String, JdbcConnectionConfig> jdbcConnections,
                                      Map<String, MongoConnectionConfig> mongoConnections,
                                      Map<String, MongoClient> mongoClients,
                                      Map<String, ElasticsearchConnectionConfig> elasticsearchConnections,
                                      Map<String, RestClient> elasticsearchClients,
                                      Map<String, DynamoDbConnectionConfig> dynamodbConnections,
                                      Map<String, DynamoDbClient> dynamodbClients,
                                      String schemaSdl,
                                      List<QueryConfig> queries,
                                      List<PostgresMutationConfig> postgresMutations,
                                      List<PostgresSubscriptionConfig> postgresSubscriptions,
                                      PostgresReplicationManager replicationManager,
                                      Map<String, KafkaProducer<String, String>> kafkaProducers,
                                      Map<String, KafkaEventConfig> kafkaEvents,
                                      List<KafkaSubscriptionConfig> subscriptions,
                                      List<KafkaConnectionConfig> kafkaConnections,
                                      AuthConfig authConfig) {
    if (schemaSdl == null || schemaSdl.isBlank()) {
      throw new IllegalStateException("GraphQL schema is required");
    }
    SchemaParser parser = new SchemaParser();
    TypeDefinitionRegistry registry = parser.parse(schemaSdl);
    RuntimeWiring.Builder wiringBuilder = RuntimeWiring.newRuntimeWiring()
      .scalar(ExtendedScalars.Json);

    Map<String, KafkaConnectionConfig> connectionIndex = indexKafkaConnections(kafkaConnections);
    String defaultConnectionRef = defaultConnectionRef(connectionIndex);

    registerQueryFetchers(vertx, registry, wiringBuilder, pools, duckdbConnections, jdbcConnections,
      mongoConnections, mongoClients, elasticsearchConnections, elasticsearchClients, dynamodbConnections,
      dynamodbClients, queries);
    registerKafkaFetchers(registry, wiringBuilder, kafkaProducers, kafkaEvents);
    registerPostgresMutationFetchers(vertx, registry, wiringBuilder, pools, postgresMutations);
    registerMissingMutationFetchers(registry, wiringBuilder, kafkaEvents, postgresMutations);
    registerPostgresSubscriptionFetchers(
      vertx, registry, wiringBuilder, postgresSubscriptions, replicationManager);
    registerSubscriptionFetchers(
      vertx, registry, wiringBuilder, subscriptions, connectionIndex, defaultConnectionRef);

    GraphQLSchema schema = new SchemaGenerator().makeExecutableSchema(registry, wiringBuilder.build());
    GraphQL.Builder builder = GraphQL.newGraphQL(schema);
    builder.instrumentation(new LoggingInstrumentation());
    builder.defaultDataFetcherExceptionHandler(new SanitizingDataFetcherExceptionHandler());
    if ((subscriptions != null && !subscriptions.isEmpty())
      || (postgresSubscriptions != null && !postgresSubscriptions.isEmpty())) {
      builder.subscriptionExecutionStrategy(new SubscriptionExecutionStrategy());
    }
    return builder.build();
  }

  private static VertxOptions buildVertxOptions(RuntimeConfig config) {
    VertxOptions options = new VertxOptions();
    ObservabilityConfig observability = config == null ? null : config.getObservability();
    MetricsConfig metrics = observability == null ? null : observability.getMetrics();
    if (metrics != null && Boolean.TRUE.equals(metrics.getEnabled())) {
      options.setMetricsOptions(buildMetricsOptions(metrics));
    }
    TracingConfig tracing = observability == null ? null : observability.getTracing();
    if (tracing != null && Boolean.TRUE.equals(tracing.getEnabled())) {
      OpenTelemetry openTelemetry = buildOpenTelemetry(tracing);
      if (openTelemetry != null) {
        options.setTracingOptions(new OpenTelemetryOptions(openTelemetry));
      }
    }
    return options;
  }

  private static MicrometerMetricsOptions buildMetricsOptions(MetricsConfig metrics) {
    MicrometerMetricsOptions options = new MicrometerMetricsOptions();
    options.setEnabled(true);
    options.setJvmMetricsEnabled(Boolean.TRUE.equals(metrics.getJvm()));
    if (Boolean.FALSE.equals(metrics.getHttpServer())) {
      options.addDisabledMetricsCategory(MetricsDomain.HTTP_SERVER);
    }
    if (Boolean.FALSE.equals(metrics.getHttpClient())) {
      options.addDisabledMetricsCategory(MetricsDomain.HTTP_CLIENT);
    }

    MeterRegistry registry = buildMeterRegistry(metrics);
    if (registry != null) {
      options.setMicrometerRegistry(registry);
    } else {
      VertxPrometheusOptions prom = new VertxPrometheusOptions().setEnabled(true);
      options.setPrometheusOptions(prom);
    }
    return options;
  }

  private static MeterRegistry buildMeterRegistry(MetricsConfig metrics) {
    if (metrics.getBackend() == MetricsConfig.Backend.OTLP) {
      OtlpMetricsConfig otlp = metrics.getOtlp();
      if (otlp == null || otlp.getEndpoint() == null || otlp.getEndpoint().isBlank()) {
        LOGGER.warn("Metrics OTLP backend enabled but endpoint missing; falling back to Prometheus");
        return new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
      }
      OtlpConfig config = new OtlpConfig() {
        @Override
        public String get(String key) {
          return null;
        }

        @Override
        public String url() {
          return otlp.getEndpoint();
        }

        @Override
        public Map<String, String> headers() {
          return otlp.getHeaders() == null ? Collections.emptyMap() : otlp.getHeaders();
        }

        @Override
        public Duration step() {
          int seconds = otlp.getStepSeconds() == null ? 10 : otlp.getStepSeconds();
          return Duration.ofSeconds(seconds);
        }
      };
      OtlpMeterRegistry registry = new OtlpMeterRegistry(config, io.micrometer.core.instrument.Clock.SYSTEM);
      applyCommonTags(registry, metrics);
      return registry;
    }
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    applyCommonTags(registry, metrics);
    return registry;
  }

  private static void applyCommonTags(MeterRegistry registry, MetricsConfig metrics) {
    if (registry == null || metrics == null || metrics.getLabels() == null) {
      return;
    }
    List<String> tags = new ArrayList<>();
    for (Map.Entry<String, String> entry : metrics.getLabels().entrySet()) {
      if (entry.getKey() == null || entry.getValue() == null) {
        continue;
      }
      tags.add(entry.getKey());
      tags.add(entry.getValue());
    }
    if (!tags.isEmpty()) {
      registry.config().commonTags(tags.toArray(new String[0]));
    }
  }

  private static OpenTelemetry buildOpenTelemetry(TracingConfig tracing) {
    if (tracing == null) {
      return null;
    }
    SpanExporter exporter = buildSpanExporter(tracing);
    if (exporter == null) {
      LOGGER.warn("Tracing enabled but exporter not configured");
      return null;
    }
    Sampler sampler = buildSampler(tracing.getSampling());
    String serviceName = tracing.getServiceName();
    Resource resource = Resource.getDefault();
    if (serviceName != null && !serviceName.isBlank()) {
      resource = resource.merge(Resource.create(
        Attributes.of(AttributeKey.stringKey("service.name"), serviceName)));
    }
    SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
      .setResource(resource)
      .setSampler(sampler)
      .addSpanProcessor(BatchSpanProcessor.builder(exporter).build())
      .build();
    return OpenTelemetrySdk.builder().setTracerProvider(tracerProvider).build();
  }

  private static SpanExporter buildSpanExporter(TracingConfig tracing) {
    if (tracing.getExporter() != TracingConfig.Exporter.OTLP) {
      LOGGER.warn("Tracing exporter {} not implemented; using OTLP", tracing.getExporter());
    }
    OtlpTracingConfig otlp = tracing.getOtlp();
    if (otlp == null || otlp.getEndpoint() == null || otlp.getEndpoint().isBlank()) {
      return null;
    }
    if (otlp.getProtocol() == OtlpTracingConfig.Protocol.HTTP_PROTOBUF) {
      OtlpHttpSpanExporterBuilder builder = OtlpHttpSpanExporter.builder().setEndpoint(otlp.getEndpoint());
      if (otlp.getHeaders() != null) {
        otlp.getHeaders().forEach(builder::addHeader);
      }
      return builder.build();
    }
    OtlpGrpcSpanExporterBuilder builder = OtlpGrpcSpanExporter.builder().setEndpoint(otlp.getEndpoint());
    if (otlp.getHeaders() != null) {
      otlp.getHeaders().forEach(builder::addHeader);
    }
    return builder.build();
  }

  private static Sampler buildSampler(SamplingConfig sampling) {
    if (sampling == null) {
      return Sampler.alwaysOn();
    }
    switch (sampling.getType()) {
      case ALWAYS_OFF:
        return Sampler.alwaysOff();
      case RATIO:
        double ratio = sampling.getValue() == null ? 0.1 : sampling.getValue();
        if (ratio < 0) {
          ratio = 0;
        }
        if (ratio > 1) {
          ratio = 1;
        }
        return Sampler.traceIdRatioBased(ratio);
      case ALWAYS_ON:
      default:
        return Sampler.alwaysOn();
    }
  }

  private static void configureObservabilityRoutes(Vertx vertx,
                                                   Router router,
                                                   ObservabilityConfig observability,
                                                   Map<String, PgPool> pools,
                                                   Map<String, DuckDbConnectionConfig> duckdbConnections,
                                                   Map<String, KafkaConnectionConfig> kafkaConnections) {
    if (observability == null || Boolean.FALSE.equals(observability.getEnabled())) {
      return;
    }
    MetricsConfig metrics = observability.getMetrics();
    if (metrics != null && Boolean.TRUE.equals(metrics.getEnabled())) {
      String endpoint = metrics.getEndpoint();
      if (endpoint == null || endpoint.isBlank()) {
        endpoint = "/metrics";
      }
      router.get(endpoint).handler(ctx -> {
        MeterRegistry registry = BackendRegistries.getDefaultNow();
        if (registry instanceof PrometheusMeterRegistry) {
          ctx.response().putHeader("Content-Type", "text/plain; version=0.0.4")
            .end(((PrometheusMeterRegistry) registry).scrape());
        } else {
          ctx.response().setStatusCode(503).end("Metrics backend not Prometheus");
        }
      });
    }

    HealthConfig health = observability.getHealth();
    if (health != null && Boolean.TRUE.equals(health.getEnabled())) {
      String endpoint = health.getEndpoint();
      if (endpoint == null || endpoint.isBlank()) {
        endpoint = "/healthz";
      }
      HealthChecks checks = HealthChecks.create(vertx);
      if (health.getChecks() != null) {
        for (HealthCheckConfig check : health.getChecks()) {
          if (check == null || check.getType() == null) {
            continue;
          }
          String name = check.getName() == null || check.getName().isBlank()
            ? check.getType().name().toLowerCase(Locale.ROOT)
            : check.getName();
          switch (check.getType()) {
            case JDBC:
              checks.register(name, promise -> {
                PgPool pool = pools.get(check.getConnectionRef());
                if (pool == null) {
                  promise.fail("missing pool");
                  return;
                }
                pool.query("select 1").execute(ar -> {
                  if (ar.failed()) {
                    promise.fail(ar.cause());
                  } else {
                    promise.complete();
                  }
                });
              });
              break;
            case DUCKDB:
              checks.register(name, promise -> {
                DuckDbConnectionConfig conn = duckdbConnections.get(check.getConnectionRef());
                if (conn == null) {
                  promise.fail("missing duckdb connection");
                  return;
                }
                vertx.executeBlocking(block -> {
                  try (Connection db = openDuckDbConnection(conn)) {
                    try (Statement stmt = db.createStatement()) {
                      stmt.execute("select 1");
                    }
                    block.complete();
                  } catch (Exception e) {
                    block.fail(e);
                  }
                }, ar -> {
                  if (ar.failed()) {
                    promise.fail(ar.cause());
                  } else {
                    promise.complete();
                  }
                });
              });
              break;
            case KAFKA:
              checks.register(name, promise -> {
                KafkaConnectionConfig conn = kafkaConnections.get(check.getConnectionRef());
                if (conn == null) {
                  promise.fail("missing kafka connection");
                  return;
                }
                Properties props = new Properties();
                props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, conn.getBootstrapServers());
                props.put(AdminClientConfig.CLIENT_ID_CONFIG,
                  conn.getClientId() == null ? "healthcheck" : conn.getClientId() + "-health");
                try (AdminClient admin = AdminClient.create(props)) {
                  admin.listTopics().names().get(3, TimeUnit.SECONDS);
                  promise.complete();
                } catch (Exception e) {
                  promise.fail(e);
                }
              });
              break;
            default:
              break;
          }
        }
      }
      HealthCheckHandler handler = HealthCheckHandler.createWithHealthChecks(checks);
      router.get(endpoint).handler(handler);
    }
  }

  private static void registerQueryFetchers(Vertx vertx,
                                            TypeDefinitionRegistry registry,
                                            RuntimeWiring.Builder wiringBuilder,
                                            Map<String, PgPool> pools,
                                            Map<String, DuckDbConnectionConfig> duckdbConnections,
                                            Map<String, JdbcConnectionConfig> jdbcConnections,
                                            Map<String, MongoConnectionConfig> mongoConnections,
                                            Map<String, MongoClient> mongoClients,
                                            Map<String, ElasticsearchConnectionConfig> elasticsearchConnections,
                                            Map<String, RestClient> elasticsearchClients,
                                            Map<String, DynamoDbConnectionConfig> dynamodbConnections,
                                            Map<String, DynamoDbClient> dynamodbClients,
                                            List<QueryConfig> queries) {
    if (queries == null || queries.isEmpty()) {
      return;
    }
    for (QueryConfig query : queries) {
      if (query == null || query.getName() == null || query.getName().isBlank()) {
        continue;
      }
      String ownerType = query.getTypeName();
      if (ownerType == null || ownerType.isBlank()) {
        ownerType = findFieldOwner(registry, query.getName());
      }
      if (!fieldExists(registry, ownerType, query.getName())) {
        throw new IllegalStateException("Field " + query.getName() + " not found on type " + ownerType);
      }
      wiringBuilder.type(TypeRuntimeWiring.newTypeWiring(ownerType)
        .dataFetcher(query.getName(), env -> executeQuery(vertx, pools, duckdbConnections, jdbcConnections,
          mongoConnections, mongoClients, elasticsearchConnections, elasticsearchClients, dynamodbConnections,
          dynamodbClients, query, env)));
    }
  }

  private static void registerKafkaFetchers(TypeDefinitionRegistry registry,
                                            RuntimeWiring.Builder wiringBuilder,
                                            Map<String, KafkaProducer<String, String>> producers,
                                            Map<String, KafkaEventConfig> events) {
    if (producers.isEmpty() || events.isEmpty()) {
      return;
    }
    for (Map.Entry<String, KafkaEventConfig> entry : events.entrySet()) {
      String fieldName = entry.getKey();
      KafkaEventConfig event = entry.getValue();
      if (event == null || fieldName == null || fieldName.isBlank()) {
        continue;
      }
      if (!fieldExists(registry, "Mutation", fieldName)) {
        throw new IllegalStateException("Mutation field " + fieldName + " not found in schema");
      }
      wiringBuilder.type(TypeRuntimeWiring.newTypeWiring("Mutation")
        .dataFetcher(fieldName, env -> {
          AuthorizationDecision decision = authorizeOperation(
            "mutation",
            fieldName,
            event.getRequiredRoles(),
            event.getRequiredScopes(),
            env);
          if (!decision.isAllowed()) {
            throw new IllegalStateException(decision.getMessage());
          }
          Object payload = env.getArgument("event");
          if (payload == null && !env.getArguments().isEmpty()) {
            payload = env.getArguments().values().iterator().next();
          }
          return publishKafkaEvent(event, producers, payload);
        }));
    }
  }

  private static void registerPostgresMutationFetchers(Vertx vertx,
                                                       TypeDefinitionRegistry registry,
                                                       RuntimeWiring.Builder wiringBuilder,
                                                       Map<String, PgPool> pools,
                                                       List<PostgresMutationConfig> mutations) {
    if (mutations == null || mutations.isEmpty()) {
      return;
    }
    if (pools.isEmpty()) {
      throw new IllegalStateException("Postgres mutations configured without a Postgres pool");
    }
    for (PostgresMutationConfig mutation : mutations) {
      if (mutation == null || mutation.getName() == null || mutation.getName().isBlank()) {
        continue;
      }
      if (!fieldExists(registry, "Mutation", mutation.getName())) {
        throw new IllegalStateException("Mutation field " + mutation.getName() + " not found in schema");
      }
      wiringBuilder.type(TypeRuntimeWiring.newTypeWiring("Mutation")
        .dataFetcher(mutation.getName(), env -> {
          AuthorizationDecision decision = authorizeOperation(
            "mutation",
            mutation.getName(),
            mutation.getRequiredRoles(),
            mutation.getRequiredScopes(),
            env);
          if (!decision.isAllowed()) {
            throw new IllegalStateException(decision.getMessage());
          }
          return executePostgresMutation(vertx, pools, mutation, env);
        }));
    }
  }

  private static void registerMissingMutationFetchers(TypeDefinitionRegistry registry,
                                                      RuntimeWiring.Builder wiringBuilder,
                                                      Map<String, KafkaEventConfig> events,
                                                      List<PostgresMutationConfig> mutations) {
    Set<String> configured = new HashSet<>();
    if (events != null) {
      configured.addAll(events.keySet());
    }
    if (mutations != null) {
      for (PostgresMutationConfig mutation : mutations) {
        if (mutation != null && mutation.getName() != null && !mutation.getName().isBlank()) {
          configured.add(mutation.getName());
        }
      }
    }
    registry.getType("Mutation", ObjectTypeDefinition.class).ifPresent(type -> {
      for (FieldDefinition field : type.getFieldDefinitions()) {
        String fieldName = field.getName();
        if (configured.contains(fieldName)) {
          continue;
        }
        wiringBuilder.type(TypeRuntimeWiring.newTypeWiring("Mutation")
          .dataFetcher(fieldName, env -> {
            String message = "No mutation configured for field " + fieldName;
            LOGGER.error(message);
            throw new IllegalStateException(message);
          }));
      }
    });
  }

  private static void registerPostgresSubscriptionFetchers(Vertx vertx,
                                                           TypeDefinitionRegistry registry,
                                                           RuntimeWiring.Builder wiringBuilder,
                                                           List<PostgresSubscriptionConfig> subscriptions,
                                                           PostgresReplicationManager replicationManager) {
    if (subscriptions == null || subscriptions.isEmpty()) {
      return;
    }
    if (registry.getType("Subscription", ObjectTypeDefinition.class).isEmpty()) {
      LOGGER.warn("Postgres subscriptions configured but schema has no Subscription type");
      return;
    }
    for (PostgresSubscriptionConfig subscription : subscriptions) {
      if (subscription == null) {
        continue;
      }
      String fieldName = subscription.getFieldName();
      if (fieldName == null || fieldName.isBlank()) {
        continue;
      }
      if (!fieldExists(registry, "Subscription", fieldName)) {
        throw new IllegalStateException("Subscription field " + fieldName + " not found in schema");
      }
      wiringBuilder.type(TypeRuntimeWiring.newTypeWiring("Subscription")
        .dataFetcher(fieldName, env -> {
          AuthorizationDecision decision = authorizeOperation(
            "subscription",
            fieldName,
            subscription.getRequiredRoles(),
            subscription.getRequiredScopes(),
            env);
          if (!decision.isAllowed()) {
            throw new IllegalStateException(decision.getMessage());
          }
          return new PostgresSubscriptionPublisher(vertx, subscription, replicationManager, env);
        }));
    }
  }

  private static void registerSubscriptionFetchers(Vertx vertx,
                                                   TypeDefinitionRegistry registry,
                                                   RuntimeWiring.Builder wiringBuilder,
                                                   List<KafkaSubscriptionConfig> subscriptions,
                                                   Map<String, KafkaConnectionConfig> connectionIndex,
                                                   String defaultConnectionRef) {
    if (subscriptions == null || subscriptions.isEmpty()) {
      return;
    }
    if (registry.getType("Subscription", ObjectTypeDefinition.class).isEmpty()) {
      LOGGER.warn("Kafka subscriptions configured but schema has no Subscription type");
      return;
    }
    if (connectionIndex.isEmpty()) {
      throw new IllegalStateException("Kafka subscriptions configured without kafka connections");
    }
    for (KafkaSubscriptionConfig subscription : subscriptions) {
      if (subscription == null) {
        continue;
      }
      String fieldName = subscription.getFieldName();
      if (fieldName == null || fieldName.isBlank()) {
        continue;
      }
      if (subscription.getTopic() == null || subscription.getTopic().isBlank()) {
        throw new IllegalStateException("Subscription " + fieldName + " missing topic");
      }
      if (!fieldExists(registry, "Subscription", fieldName)) {
        throw new IllegalStateException("Subscription field " + fieldName + " not found in schema");
      }
      wiringBuilder.type(TypeRuntimeWiring.newTypeWiring("Subscription")
        .dataFetcher(fieldName, env -> {
          AuthorizationDecision decision = authorizeOperation(
            "subscription",
            fieldName,
            subscription.getRequiredRoles(),
            subscription.getRequiredScopes(),
            env);
          if (!decision.isAllowed()) {
            throw new IllegalStateException(decision.getMessage());
          }
          return new KafkaSubscriptionPublisher(vertx, subscription, connectionIndex, defaultConnectionRef, env);
        }));
    }
  }

  private static String findFieldOwner(TypeDefinitionRegistry registry, String fieldName) {
    List<ObjectTypeDefinition> types = registry.getTypes(ObjectTypeDefinition.class);
    List<String> matches = new ArrayList<>();
    for (ObjectTypeDefinition type : types) {
      for (FieldDefinition field : type.getFieldDefinitions()) {
        if (fieldName.equals(field.getName())) {
          matches.add(type.getName());
        }
      }
    }
    if (matches.isEmpty()) {
      throw new IllegalStateException("Field " + fieldName + " not found in schema");
    }
    if (matches.size() > 1) {
      throw new IllegalStateException("Field " + fieldName + " found on multiple types: " + matches);
    }
    return matches.get(0);
  }

  private static boolean fieldExists(TypeDefinitionRegistry registry, String typeName, String fieldName) {
    return registry.getType(typeName, ObjectTypeDefinition.class)
      .map(type -> type.getFieldDefinitions().stream().anyMatch(field -> fieldName.equals(field.getName())))
      .orElse(false);
  }

  private static String graphqlSafeName(String name) {
    if (name == null || name.isBlank()) {
      return "event";
    }
    StringBuilder builder = new StringBuilder(name.length());
    char first = name.charAt(0);
    if (isGraphqlNameStart(first)) {
      builder.append(first);
    } else {
      builder.append('e');
      if (isGraphqlNamePart(first)) {
        builder.append(first);
      } else {
        builder.append('_');
      }
    }
    for (int i = 1; i < name.length(); i++) {
      char ch = name.charAt(i);
      builder.append(isGraphqlNamePart(ch) ? ch : '_');
    }
    return builder.toString();
  }

  private static boolean isGraphqlNameStart(char ch) {
    return ch == '_' || (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z');
  }

  private static boolean isGraphqlNamePart(char ch) {
    return isGraphqlNameStart(ch) || (ch >= '0' && ch <= '9');
  }

  private static CompletionStage<String> publishKafkaEvent(KafkaEventConfig event,
                                                           Map<String, KafkaProducer<String, String>> producers,
                                                           Object payloadArg) {
    KafkaProducer<String, String> producer = producers.get(event.getConnectionRef());
    if (producer == null) {
      LOGGER.error(
          "Kafka producer missing for event={} connection={} topic={}",
          event.getName(),
          event.getConnectionRef(),
          event.getTopic());
      CompletableFuture<String> failed = new CompletableFuture<>();
      failed.completeExceptionally(new IllegalStateException("Missing Kafka connection"));
      return failed;
    }
    Object enrichedPayload = applyEnrichment(event, payloadArg);
    JsonObject payloadObject = toJsonObject(enrichedPayload);
    String key = null;
    if (payloadObject != null) {
      key = resolveKafkaEventKey(event, payloadObject);
    }
    String payload = encodeEventPayload(enrichedPayload);
    KafkaProducerRecord<String, String> record = KafkaProducerRecord.create(event.getTopic(), key, payload);
    CompletableFuture<String> future = new CompletableFuture<>();
    producer.send(record, ar -> {
      if (ar.failed()) {
        LOGGER.error(
            "Kafka publish failed for event={} topic={} payload={}",
            event.getName(),
            event.getTopic(),
            payload,
            ar.cause());
        future.completeExceptionally(ar.cause());
      } else {
        future.complete("queued");
      }
    });
    return future;
  }

  private static CompletionStage<Object> executePostgresMutation(Vertx vertx,
                                                                 Map<String, PgPool> pools,
                                                                 PostgresMutationConfig mutation,
                                                                 DataFetchingEnvironment env) {
    CompletableFuture<Object> future = new CompletableFuture<>();
    PgPool pool = selectPostgresPool(mutation, pools);
    if (pool == null) {
      future.completeExceptionally(new IllegalStateException("No Postgres pool available"));
      return future;
    }

    MutationPayload payload = buildMutationPayload(env);
    Object enrichedPayload = applyEnrichment(mutation, payload.payload);
    CompletionStage<List<Object>> valuesStage = resolveMutationValuesAsync(vertx, mutation, env, enrichedPayload, payload.argName);
    toCompletableFuture(valuesStage).whenComplete((values, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
        return;
      }
      if (values == null || values.isEmpty()) {
        values = List.of(enrichedPayload);
      }
      Tuple tuple = Tuple.tuple(values);
      boolean hasPayloadArg = payload.argName != null;
      Integer timeoutMs = mutation.getTimeoutMs();
      if (timeoutMs != null && timeoutMs > 0) {
        executePgPreparedWithStatementTimeout(pool, mutation.getSql(), tuple, timeoutMs, ar ->
          handleMutationResult(ar, future, enrichedPayload, hasPayloadArg));
      } else {
        pool.preparedQuery(mutation.getSql()).execute(tuple, ar ->
          handleMutationResult(ar, future, enrichedPayload, hasPayloadArg));
      }
    });
    return future;
  }

  private static String resolveKafkaEventKey(KafkaEventConfig event, JsonObject payload) {
    if (event == null || payload == null) {
      return null;
    }
    java.util.List<String> fields = event.getPartitionKeyFields();
    if (fields != null && !fields.isEmpty()) {
      JsonObject key = new JsonObject();
      for (String field : fields) {
        if (field == null || field.isBlank()) {
          continue;
        }
        Object value = payload.getValue(field);
        if (value == null) {
          return null;
        }
        key.put(field, value);
      }
      return key.encode();
    }
    return resolvePartitionKey(event.getPartitionKeyField(), payload);
  }

  private static Object applyEnrichment(KafkaEventConfig event, Object payloadArg) {
    List<EnrichFieldConfig> enrich = event.getEnrich();
    if (enrich == null || enrich.isEmpty()) {
      return payloadArg;
    }

    JsonObject payload = toJsonObject(payloadArg);
    if (payload == null) {
      LOGGER.warn("Kafka event payload is not an object; skipping enrich for event={}", event.getName());
      return payloadArg;
    }

    Map<String, Supplier<Object>> registry = buildEnrichRegistry();
    for (EnrichFieldConfig field : enrich) {
      if (field == null || field.getName() == null || field.getName().isBlank()) {
        continue;
      }
      String fn = field.getFn();
      if (fn == null || fn.isBlank()) {
        continue;
      }
      Supplier<Object> supplier = registry.get(fn);
      if (supplier == null) {
        LOGGER.warn("Unknown enrich function '{}' for event={}", fn, event.getName());
        continue;
      }
      payload.put(field.getName(), supplier.get());
    }

    return payload;
  }

  private static Object applyEnrichment(PostgresMutationConfig mutation, Object payloadArg) {
    List<EnrichFieldConfig> enrich = mutation.getEnrich();
    if (enrich == null || enrich.isEmpty()) {
      return payloadArg;
    }

    JsonObject payload = toJsonObject(payloadArg);
    if (payload == null) {
      LOGGER.warn("Postgres mutation payload is not an object; skipping enrich for mutation={}", mutation.getName());
      return payloadArg;
    }

    Map<String, Supplier<Object>> registry = buildEnrichRegistry();
    for (EnrichFieldConfig field : enrich) {
      if (field == null || field.getName() == null || field.getName().isBlank()) {
        continue;
      }
      String fn = field.getFn();
      if (fn == null || fn.isBlank()) {
        continue;
      }
      Supplier<Object> supplier = registry.get(fn);
      if (supplier == null) {
        LOGGER.warn("Unknown enrich function '{}' for mutation={}", fn, mutation.getName());
        continue;
      }
      payload.put(field.getName(), supplier.get());
    }

    return payload;
  }

  private static JsonObject toJsonObject(Object payloadArg) {
    if (payloadArg == null) {
      return new JsonObject();
    }
    if (payloadArg instanceof JsonObject) {
      return (JsonObject) payloadArg;
    }
    if (payloadArg instanceof Map) {
      return new JsonObject((Map<String, Object>) payloadArg);
    }
    return null;
  }

  private static Map<String, Supplier<Object>> buildEnrichRegistry() {
    Map<String, Supplier<Object>> registry = new HashMap<>();
    registry.put("uuid", () -> java.util.UUID.randomUUID().toString());
    registry.put(
        "utcNow",
        () -> java.time.OffsetDateTime.now(java.time.ZoneId.of("UTC")).toString());
    return registry;
  }

  private static MutationPayload buildMutationPayload(DataFetchingEnvironment env) {
    if (env == null) {
      return new MutationPayload(null, null);
    }
    if (env.getArgument("event") != null || env.getArguments().containsKey("event")) {
      return new MutationPayload("event", env.getArgument("event"));
    }
    if (!env.getArguments().isEmpty()) {
      String name = env.getArguments().keySet().iterator().next();
      return new MutationPayload(name, env.getArguments().get(name));
    }
    return new MutationPayload(null, null);
  }

  private static List<Object> resolveMutationValues(PostgresMutationConfig mutation,
                                                    DataFetchingEnvironment env,
                                                    Object enrichedPayload,
                                                    String payloadArgName) {
    if (mutation.getParams() == null || mutation.getParams().isEmpty()) {
      return new ArrayList<>();
    }
    List<QueryParamConfig> params = new ArrayList<>(mutation.getParams());
    params.sort(Comparator.comparingInt(QueryParamConfig::getIndex));
    int maxIndex = params.stream().mapToInt(QueryParamConfig::getIndex).max().orElse(0);
    List<Object> values = new ArrayList<>(Collections.nCopies(maxIndex, null));
    for (QueryParamConfig param : params) {
      if (param.getIndex() <= 0 || param.getSource() == null) {
        continue;
      }
      Object value = resolveMutationParamValue(env, param.getSource(), enrichedPayload, payloadArgName);
      values.set(param.getIndex() - 1, value);
    }
    return values;
  }

  private static Object resolveMutationParamValue(DataFetchingEnvironment env,
                                                  ParamSourceConfig source,
                                                  Object enrichedPayload,
                                                  String payloadArgName) {
    if (source.getKind() == ParamSourceConfig.Kind.ARG && payloadArgName != null) {
      if (payloadArgName.equals(source.getName())) {
        return enrichedPayload;
      }
    }
    return resolveParamValue(env, source);
  }

  private static CompletionStage<List<Object>> resolveMutationValuesAsync(Vertx vertx,
                                                                          PostgresMutationConfig mutation,
                                                                          DataFetchingEnvironment env,
                                                                          Object enrichedPayload,
                                                                          String payloadArgName) {
    if (mutation == null || mutation.getParams() == null || mutation.getParams().isEmpty()) {
      return CompletableFuture.completedFuture(new ArrayList<>());
    }
    List<QueryParamConfig> params = new ArrayList<>(mutation.getParams());
    params.sort(Comparator.comparingInt(QueryParamConfig::getIndex));
    int maxIndex = params.stream().mapToInt(QueryParamConfig::getIndex).max().orElse(0);
    List<CompletionStage<Object>> stages =
      new ArrayList<>(Collections.nCopies(maxIndex, CompletableFuture.completedFuture(null)));
    for (QueryParamConfig param : params) {
      if (param == null || param.getIndex() <= 0 || param.getSource() == null) {
        continue;
      }
      CompletionStage<Object> stage = resolveMutationParamValueAsync(vertx, env, param.getSource(), enrichedPayload, payloadArgName)
        .thenApply(GraphQLServer::normalizeSqlParam);
      stages.set(param.getIndex() - 1, stage);
    }
    CompletableFuture<?>[] cfs = stages.stream().map(GraphQLServer::toCompletableFuture).toArray(CompletableFuture[]::new);
    return CompletableFuture.allOf(cfs).thenApply(ignored -> {
      List<Object> values = new ArrayList<>(Collections.nCopies(maxIndex, null));
      for (int i = 0; i < stages.size(); i++) {
        values.set(i, toCompletableFuture(stages.get(i)).join());
      }
      return values;
    });
  }

  private static CompletionStage<Object> resolveMutationParamValueAsync(Vertx vertx,
                                                                        DataFetchingEnvironment env,
                                                                        ParamSourceConfig source,
                                                                        Object enrichedPayload,
                                                                        String payloadArgName) {
    if (source != null && source.getKind() == ParamSourceConfig.Kind.ARG && payloadArgName != null) {
      if (payloadArgName.equals(source.getName())) {
        return CompletableFuture.completedFuture(enrichedPayload);
      }
    }
    return resolveParamValueAsync(vertx, env, source);
  }

  private static final class KafkaSubscriptionPublisher implements Publisher<Object> {
    private final Vertx vertx;
    private final KafkaSubscriptionConfig subscription;
    private final Map<String, KafkaConnectionConfig> connectionIndex;
    private final String defaultConnectionRef;
    private final DataFetchingEnvironment env;

    private KafkaSubscriptionPublisher(Vertx vertx,
                                       KafkaSubscriptionConfig subscription,
                                       Map<String, KafkaConnectionConfig> connectionIndex,
                                       String defaultConnectionRef,
                                       DataFetchingEnvironment env) {
      this.vertx = vertx;
      this.subscription = subscription;
      this.connectionIndex = connectionIndex;
      this.defaultConnectionRef = defaultConnectionRef;
      this.env = env;
    }

    @Override
    public void subscribe(Subscriber<? super Object> subscriber) {
      LOGGER.info("Subscription {} received subscriber {}", subscription.getFieldName(), subscriber.getClass().getName());
      KafkaConnectionConfig connection = resolveConnection(subscription, connectionIndex, defaultConnectionRef);
      if (connection == null) {
        subscriber.onError(new IllegalStateException("Kafka connection not configured"));
        return;
      }
      String groupId = "gql-" + graphqlSafeName(subscription.getFieldName()) + "-" + java.util.UUID.randomUUID();
      Map<String, String> config = new HashMap<>();
      config.put("bootstrap.servers", connection.getBootstrapServers());
      config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
      config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
      config.put("group.id", groupId);
      config.put("auto.offset.reset", "latest");
      config.put("enable.auto.commit", "true");
      if (connection.getClientId() != null && !connection.getClientId().isBlank()) {
        config.put("client.id", connection.getClientId());
      }

      KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, config);
      AtomicBoolean cancelled = new AtomicBoolean(false);
      subscriber.onSubscribe(new Subscription() {
        @Override
        public void request(long n) {
          // no backpressure support
        }

        @Override
        public void cancel() {
          LOGGER.info("Subscription {} cancelled", subscription.getFieldName());
          cancelled.set(true);
          consumer.close();
        }
      });

      consumer.subscribe(subscription.getTopic(), ar -> {
        if (ar.failed()) {
          LOGGER.error("Subscription {} failed to subscribe to topic {}", subscription.getFieldName(), subscription.getTopic(), ar.cause());
          subscriber.onError(ar.cause());
        } else {
          LOGGER.info("Subscription {} listening on topic {}", subscription.getFieldName(), subscription.getTopic());
        }
      });
      consumer.exceptionHandler(error -> {
        if (!cancelled.get()) {
          LOGGER.error("Subscription {} consumer error", subscription.getFieldName(), error);
          subscriber.onError(error);
        }
      });

      consumer.handler(record -> handleSubscriptionRecord(subscriber, subscription, env, record, cancelled));
    }
  }

  private static final class PostgresSubscriptionPublisher implements Publisher<Object> {
    private final Vertx vertx;
    private final PostgresSubscriptionConfig subscription;
    private final PostgresReplicationManager replicationManager;
    private final DataFetchingEnvironment env;

    private PostgresSubscriptionPublisher(Vertx vertx,
                                          PostgresSubscriptionConfig subscription,
                                          PostgresReplicationManager replicationManager,
                                          DataFetchingEnvironment env) {
      this.vertx = vertx;
      this.subscription = subscription;
      this.replicationManager = replicationManager;
      this.env = env;
    }

    @Override
    public void subscribe(Subscriber<? super Object> subscriber) {
      LOGGER.info("Postgres subscription {} received subscriber {}", subscription.getFieldName(),
        subscriber.getClass().getName());
      if (subscription.getConnectionRef() == null || subscription.getConnectionRef().isBlank()) {
        subscriber.onError(new IllegalStateException("Postgres subscription missing connectionRef"));
        return;
      }
      PostgresReplicationManager.SubscriptionHandle handle =
        replicationManager.subscribe(subscription, env, subscriber);
      subscriber.onSubscribe(handle);
    }
  }

  private static final class LoggingInstrumentation extends SimpleInstrumentation {
    @Override
    public CompletableFuture<ExecutionResult> instrumentExecutionResult(
        ExecutionResult executionResult,
        InstrumentationExecutionParameters parameters,
        InstrumentationState state) {
      Object data = executionResult.getData();
      if (data instanceof Publisher) {
        Publisher<?> publisher = (Publisher<?>) data;
        Publisher<Object> wrapped = subscriber -> publisher.subscribe(new Subscriber<Object>() {
          @Override
          public void onSubscribe(Subscription subscription) {
            subscriber.onSubscribe(subscription);
          }

          @Override
          public void onNext(Object item) {
            if (item instanceof ExecutionResult) {
              ExecutionResult result = (ExecutionResult) item;
              LOGGER.info("Subscription payload data={} errors={}", result.getData(), result.getErrors());
            } else {
              LOGGER.info("Subscription payload type={} value={}", item.getClass().getName(), item);
            }
            subscriber.onNext(item);
          }

          @Override
          public void onError(Throwable throwable) {
            LOGGER.error("Subscription payload error", throwable);
            subscriber.onError(throwable);
          }

          @Override
          public void onComplete() {
            subscriber.onComplete();
          }
        });
        ExecutionResult wrappedResult = ExecutionResultImpl.newExecutionResult()
            .from(executionResult)
            .data(wrapped)
            .build();
        return CompletableFuture.completedFuture(wrappedResult);
      }
      return CompletableFuture.completedFuture(executionResult);
    }
  }

  private static final class SanitizingDataFetcherExceptionHandler
    implements graphql.execution.DataFetcherExceptionHandler {
    @Override
    public CompletableFuture<graphql.execution.DataFetcherExceptionHandlerResult> handleException(
      graphql.execution.DataFetcherExceptionHandlerParameters handlerParameters) {
      Throwable err = handlerParameters == null ? null : handlerParameters.getException();
      graphql.schema.DataFetchingEnvironment env =
        handlerParameters == null ? null : handlerParameters.getDataFetchingEnvironment();

      String code = "INTERNAL";
      String message = "Internal error";
      if (err instanceof java.util.concurrent.TimeoutException) {
        code = "TIMEOUT";
        message = "Timed out";
      } else if (err instanceof IllegalArgumentException) {
        code = "BAD_REQUEST";
        message = err.getMessage() == null ? "Bad request" : err.getMessage();
      } else if (err instanceof IllegalStateException) {
        code = "CONFIG_ERROR";
        message = err.getMessage() == null ? "Invalid configuration" : err.getMessage();
      }

      Map<String, Object> extensions = new HashMap<>();
      extensions.put("code", code);
      boolean debug = Boolean.parseBoolean(System.getenv().getOrDefault("DEBUG_ERRORS", "false"));
      if (debug && err != null) {
        extensions.put("exception", err.getClass().getName());
      }

      graphql.GraphQLError gqlError = graphql.GraphqlErrorBuilder.newError(env)
        .message(message)
        .extensions(extensions)
        .build();

      graphql.execution.DataFetcherExceptionHandlerResult result =
        graphql.execution.DataFetcherExceptionHandlerResult.newResult()
          .error(gqlError)
          .build();
      return CompletableFuture.completedFuture(result);
    }
  }

  private static Map<String, KafkaConnectionConfig> indexKafkaConnections(
      List<KafkaConnectionConfig> kafkaConnections) {
    Map<String, KafkaConnectionConfig> connections = new HashMap<>();
    if (kafkaConnections == null) {
      return connections;
    }
    for (KafkaConnectionConfig connection : kafkaConnections) {
      if (connection == null || connection.getName() == null || connection.getName().isBlank()) {
        continue;
      }
      connections.put(connection.getName(), connection);
    }
    return connections;
  }

  private static String defaultConnectionRef(Map<String, KafkaConnectionConfig> connections) {
    return connections.size() == 1 ? connections.keySet().iterator().next() : null;
  }

  private static KafkaConnectionConfig resolveConnection(KafkaSubscriptionConfig subscription,
                                                         Map<String, KafkaConnectionConfig> connections,
                                                         String defaultConnectionRef) {
    String ref = subscription.getConnectionRef();
    if (ref == null || ref.isBlank()) {
      if (defaultConnectionRef == null) {
        throw new IllegalStateException("Kafka subscription " + subscription.getFieldName()
          + " missing connectionRef");
      }
      ref = defaultConnectionRef;
    }
    KafkaConnectionConfig connection = connections.get(ref);
    if (connection == null) {
      throw new IllegalStateException("Kafka connection " + ref + " not configured");
    }
    return connection;
  }

  private static void handleSubscriptionRecord(Subscriber<? super Object> subscriber,
                                               KafkaSubscriptionConfig subscription,
                                               DataFetchingEnvironment env,
                                               KafkaConsumerRecord<String, String> record,
                                               AtomicBoolean cancelled) {
    if (cancelled.get()) {
      return;
    }
    Object payload = decodeSubscriptionPayload(subscription, record.value());
    if (!matchesFilters(payload, subscription.getFilters(), env)) {
      return;
    }
    try {
      subscriber.onNext(payload);
    } catch (Exception e) {
      LOGGER.error("Subscription {} failed delivering payload", subscription.getFieldName(), e);
      subscriber.onError(e);
    }
  }

  private static Object decodeSubscriptionPayload(KafkaSubscriptionConfig subscription, String value) {
    if (value == null) {
      return null;
    }
    String format = subscription.getFormat();
    if (format == null || format.isBlank() || "json".equalsIgnoreCase(format)) {
      try {
        JsonObject object = new JsonObject(value);
        return object.getMap();
      } catch (RuntimeException ignored) {
        try {
          JsonArray array = new JsonArray(value);
          return array.getList();
        } catch (RuntimeException ignoredArray) {
          return value;
        }
      }
    }
    return value;
  }

  private static boolean matchesFilters(Object payload,
                                        List<KafkaSubscriptionFilterConfig> filters,
                                        DataFetchingEnvironment env) {
    if (filters == null || filters.isEmpty()) {
      return true;
    }
    if (!(payload instanceof Map)) {
      return false;
    }
    Map<?, ?> map = (Map<?, ?>) payload;
    for (KafkaSubscriptionFilterConfig filter : filters) {
      if (filter == null || filter.getField() == null || filter.getSource() == null) {
        continue;
      }
      Object actual = map.get(filter.getField());
      Object expected = resolveParamValue(env, filter.getSource());
      if (!valuesEqual(actual, expected)) {
        return false;
      }
    }
    return true;
  }

  private static boolean matchesPostgresFilters(Object payload,
                                                List<PostgresSubscriptionFilterConfig> filters,
                                                DataFetchingEnvironment env) {
    if (filters == null || filters.isEmpty()) {
      return true;
    }
    if (!(payload instanceof Map)) {
      return false;
    }
    Map<?, ?> map = (Map<?, ?>) payload;
    for (PostgresSubscriptionFilterConfig filter : filters) {
      if (filter == null || filter.getField() == null || filter.getSource() == null) {
        continue;
      }
      Object actual = map.get(filter.getField());
      Object expected = resolveParamValue(env, filter.getSource());
      if (!valuesEqual(actual, expected)) {
        return false;
      }
    }
    return true;
  }

  private static boolean valuesEqual(Object actual, Object expected) {
    if (actual == expected) {
      return true;
    }
    if (actual == null || expected == null) {
      return false;
    }
    if (actual instanceof Number && expected instanceof Number) {
      return ((Number) actual).doubleValue() == ((Number) expected).doubleValue();
    }
    if (actual instanceof Boolean && expected instanceof Boolean) {
      return actual.equals(expected);
    }
    if (actual.getClass().equals(expected.getClass())) {
      return actual.equals(expected);
    }
    return String.valueOf(actual).equals(String.valueOf(expected));
  }

  private static final class AuthorizationDecision {
    private final boolean allowed;
    private final String message;

    private AuthorizationDecision(boolean allowed, String message) {
      this.allowed = allowed;
      this.message = message;
    }

    private static AuthorizationDecision allow() {
      return new AuthorizationDecision(true, "ok");
    }

    private static AuthorizationDecision deny(String message) {
      return new AuthorizationDecision(false, message);
    }

    private boolean isAllowed() {
      return allowed;
    }

    private String getMessage() {
      return message;
    }
  }

  private static final class AuthResult {
    private final boolean allowed;
    private final boolean missingToken;
    private final String message;
    private final AuthContext context;

    private AuthResult(boolean allowed, boolean missingToken, String message, AuthContext context) {
      this.allowed = allowed;
      this.missingToken = missingToken;
      this.message = message;
      this.context = context;
    }

    private static AuthResult allow(AuthContext context) {
      return new AuthResult(true, false, "ok", context);
    }

    private static AuthResult deny(String message, boolean missingToken) {
      return new AuthResult(false, missingToken, message, null);
    }

    private boolean isAllowed() {
      return allowed;
    }

    private boolean isMissingToken() {
      return missingToken;
    }

    private String getMessage() {
      return message;
    }

    private AuthContext getContext() {
      return context;
    }
  }

  private static final class AuthContext {
    private final String subject;
    private final Set<String> roles;
    private final Set<String> scopes;
    private final Map<String, Object> claims;

    private AuthContext(String subject, Set<String> roles, Set<String> scopes, Map<String, Object> claims) {
      this.subject = subject;
      this.roles = roles == null ? Collections.emptySet() : roles;
      this.scopes = scopes == null ? Collections.emptySet() : scopes;
      this.claims = claims == null ? Collections.emptyMap() : claims;
    }

    private String getSubject() {
      return subject;
    }

    private boolean isAuthenticated() {
      return subject != null && !subject.isBlank();
    }

    private boolean hasAnyRole(List<String> requiredRoles) {
      if (requiredRoles == null || requiredRoles.isEmpty()) {
        return true;
      }
      for (String role : requiredRoles) {
        if (role == null) {
          continue;
        }
        if (roles.contains(role)) {
          return true;
        }
      }
      return false;
    }

    private boolean hasAnyScope(List<String> requiredScopes) {
      if (requiredScopes == null || requiredScopes.isEmpty()) {
        return true;
      }
      for (String scope : requiredScopes) {
        if (scope == null) {
          continue;
        }
        if (scopes.contains(scope)) {
          return true;
        }
      }
      return false;
    }

    private Map<String, Object> getClaims() {
      return claims;
    }

    private Set<String> getRoles() {
      return roles;
    }

    private Set<String> getScopes() {
      return scopes;
    }
  }

  private static final class AuthManager {
    private final boolean enabled;
    private final boolean required;
    private final String tokenHeader;
    private final String tokenCookie;
    private final String tokenQueryParam;
    private final JwtVerifier jwtVerifier;
    private final Map<String, BasicAuthUser> basicUsers;

    private AuthManager(boolean enabled,
                        boolean required,
                        String tokenHeader,
                        String tokenCookie,
                        String tokenQueryParam,
                        JwtVerifier jwtVerifier,
                        Map<String, BasicAuthUser> basicUsers) {
      this.enabled = enabled;
      this.required = required;
      this.tokenHeader = tokenHeader;
      this.tokenCookie = tokenCookie;
      this.tokenQueryParam = tokenQueryParam;
      this.jwtVerifier = jwtVerifier;
      this.basicUsers = basicUsers == null ? Collections.emptyMap() : basicUsers;
    }

    private static AuthManager fromConfig(AuthConfig config) {
      if (config == null || config.getType() == AuthConfig.Type.NONE) {
        return new AuthManager(false, false, null, null, null, null, null);
      }
      boolean required = config.getRequired() == null || config.getRequired();
      String header = config.getTokenHeader();
      if (header == null || header.isBlank()) {
        header = "Authorization";
      }
      JwtVerifier verifier = JwtVerifier.fromConfig(config.getJwt());
      Map<String, BasicAuthUser> users = buildBasicUsers(config.getBasicUsers());
      return new AuthManager(true, required, header, config.getTokenCookie(), config.getTokenQueryParam(), verifier,
        users);
    }

    private boolean isEnabled() {
      return enabled;
    }

    private AuthResult authenticateHttp(io.vertx.ext.web.RoutingContext ctx) {
      String token = extractTokenFromHttp(ctx);
      if (token == null || token.isBlank()) {
        String rid = ctx == null ? null : ctx.get("rid");
        String method = ctx == null || ctx.request() == null || ctx.request().method() == null ? "?" : ctx.request().method().name();
        String path = ctx == null || ctx.request() == null ? "?" : ctx.request().path();
        LOGGER.info("Auth missing token (required={}) rid={} {} {}", required, rid, method, path);
        if (required) {
          return AuthResult.deny("Unauthorized", true);
        }
        return AuthResult.allow(null);
      }
      return authenticateToken(token);
    }

    private AuthResult authenticateWebSocket(Message message) {
      String token = extractTokenFromConnectionParams(message == null ? null : message.connectionParams());
      if (token == null || token.isBlank()) {
        ServerWebSocket socket = message == null ? null : message.socket();
        if (socket != null) {
          token = extractTokenFromHeaders(socket.headers());
        }
      }
      return authenticateToken(token);
    }

    private AuthResult authenticateToken(String token) {
      if (token != null && token.regionMatches(true, 0, "Basic ", 0, 6)) {
        return authenticateBasic(token);
      }
      LOGGER.info("Auth token received len={} prefix={}", token.length(), tokenPrefix(token));
      if (jwtVerifier == null) {
        LOGGER.info("Auth verifier not configured; allowing request");
        return AuthResult.allow(null);
      }
      JwtVerifier.JwtVerificationResult result = jwtVerifier.verify(token);
      if (!result.isValid()) {
        LOGGER.warn("Auth token rejected: {}", result.getMessage());
        return AuthResult.deny("Unauthorized: " + result.getMessage(), false);
      }
      AuthContext ctx = result.getContext();
      if (ctx != null) {
        LOGGER.info("Auth token accepted subject={} roles={} scopes={}",
          ctx.getSubject(), ctx.roles, ctx.scopes);
      } else {
        LOGGER.info("Auth token accepted (no context)");
      }
      return AuthResult.allow(result.getContext());
    }

    private byte[] getJwtSharedSecretBytes() {
      if (jwtVerifier == null) {
        return null;
      }
      return jwtVerifier.getSharedSecretBytes();
    }

    private AuthResult authenticateBasic(String token) {
      if (basicUsers.isEmpty()) {
        LOGGER.warn("Basic auth attempted but no users configured");
        return AuthResult.deny("Unauthorized", false);
      }
      String decoded = decodeBasicToken(token);
      if (decoded == null) {
        return AuthResult.deny("Unauthorized: invalid basic token", false);
      }
      int idx = decoded.indexOf(':');
      if (idx <= 0) {
        return AuthResult.deny("Unauthorized: invalid basic token", false);
      }
      String username = decoded.substring(0, idx);
      String password = decoded.substring(idx + 1);
      BasicAuthUser user = basicUsers.get(username);
      if (user == null || user.password == null || !user.password.equals(password)) {
        LOGGER.warn("Basic auth failed for user={}", username);
        return AuthResult.deny("Unauthorized", false);
      }
      LOGGER.info("Basic auth accepted user={} roles={}", username, user.roles);
      AuthContext context = new AuthContext(username, user.roles, Collections.emptySet(), Collections.emptyMap());
      return AuthResult.allow(context);
    }

    private String extractTokenFromHttp(io.vertx.ext.web.RoutingContext ctx) {
      if (ctx == null) {
        return null;
      }
      String token = extractTokenFromHeaders(ctx.request().headers());
      if ((token == null || token.isBlank()) && tokenQueryParam != null && !tokenQueryParam.isBlank()) {
        token = ctx.request().getParam(tokenQueryParam);
      }
      if ((token == null || token.isBlank()) && tokenCookie != null && !tokenCookie.isBlank()) {
        token = extractTokenFromCookie(ctx.request().getHeader("Cookie"), tokenCookie);
      }
      return token;
    }

    private String extractTokenFromHeaders(MultiMap headers) {
      if (headers == null) {
        return null;
      }
      String headerValue = headers.get(tokenHeader);
      if (headerValue == null || headerValue.isBlank()) {
        headerValue = headers.get("authorization");
      }
      if (headerValue != null && !headerValue.isBlank()) {
        LOGGER.info("Auth header '{}' present", tokenHeader);
      }
      return extractBearerToken(headerValue);
    }

    private String extractTokenFromConnectionParams(Object params) {
      if (params == null) {
        return null;
      }
      if (params instanceof JsonObject) {
        JsonObject obj = (JsonObject) params;
        String value = stringFromAny(obj.getValue("Authorization"));
        if (value == null) {
          value = stringFromAny(obj.getValue("authorization"));
        }
        if (value == null) {
          value = stringFromAny(obj.getValue("authToken"));
        }
        if (value == null) {
          value = stringFromAny(obj.getValue("token"));
        }
        if (value == null) {
          value = stringFromAny(obj.getValue("access_token"));
        }
        if (value != null && !value.isBlank()) {
          LOGGER.info("Auth token found in websocket connection params");
        }
        return extractBearerToken(value);
      }
      if (params instanceof Map) {
        Object raw = ((Map<?, ?>) params).get("Authorization");
        if (raw == null) {
          raw = ((Map<?, ?>) params).get("authorization");
        }
        if (raw == null) {
          raw = ((Map<?, ?>) params).get("authToken");
        }
        if (raw == null) {
          raw = ((Map<?, ?>) params).get("token");
        }
        if (raw == null) {
          raw = ((Map<?, ?>) params).get("access_token");
        }
        if (raw != null) {
          LOGGER.info("Auth token found in websocket connection params map");
        }
        return extractBearerToken(stringFromAny(raw));
      }
      return null;
    }

    private String extractBearerToken(String value) {
      if (value == null) {
        return null;
      }
      String trimmed = value.trim();
      if (trimmed.regionMatches(true, 0, "Basic ", 0, 6)) {
        return trimmed;
      }
      if (trimmed.regionMatches(true, 0, "Bearer ", 0, 7)) {
        return trimmed.substring(7).trim();
      }
      return trimmed;
    }

    private String extractTokenFromCookie(String cookieHeader, String cookieName) {
      if (cookieHeader == null || cookieHeader.isBlank()) {
        return null;
      }
      String[] parts = cookieHeader.split(";");
      for (String part : parts) {
        String[] kv = part.trim().split("=", 2);
        if (kv.length == 2 && cookieName.equals(kv[0].trim())) {
          LOGGER.info("Auth token found in cookie {}", cookieName);
          return kv[1].trim();
        }
      }
      return null;
    }

    private String tokenPrefix(String token) {
      if (token == null) {
        return "null";
      }
      int len = Math.min(8, token.length());
      return token.substring(0, len);
    }

    private String decodeBasicToken(String headerValue) {
      if (headerValue == null) {
        return null;
      }
      String trimmed = headerValue.trim();
      if (!trimmed.regionMatches(true, 0, "Basic ", 0, 6)) {
        return null;
      }
      String encoded = trimmed.substring(6).trim();
      if (encoded.isEmpty()) {
        return null;
      }
      try {
        byte[] decoded = java.util.Base64.getDecoder().decode(encoded);
        return new String(decoded, StandardCharsets.UTF_8);
      } catch (IllegalArgumentException e) {
        LOGGER.warn("Invalid basic auth base64", e);
        return null;
      }
    }

    private String stringFromAny(Object value) {
      if (value == null) {
        return null;
      }
      if (value instanceof String) {
        return (String) value;
      }
      return String.valueOf(value);
    }
  }

  private static Map<String, BasicAuthUser> buildBasicUsers(List<BasicAuthUserConfig> users) {
    if (users == null || users.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<String, BasicAuthUser> index = new HashMap<>();
    for (BasicAuthUserConfig user : users) {
      if (user == null || user.getUsername() == null || user.getUsername().isBlank()) {
        continue;
      }
      String username = user.getUsername().trim();
      String password = user.getPassword();
      if (password == null) {
        continue;
      }
      Set<String> roles = new HashSet<>();
      if (user.getRoles() != null) {
        for (String role : user.getRoles()) {
          if (role != null && !role.isBlank()) {
            roles.add(role.trim());
          }
        }
      }
      index.put(username, new BasicAuthUser(username, password, roles));
    }
    return index;
  }

  private static final class BasicAuthUser {
    private final String username;
    private final String password;
    private final Set<String> roles;

    private BasicAuthUser(String username, String password, Set<String> roles) {
      this.username = username;
      this.password = password;
      this.roles = roles == null ? Collections.emptySet() : roles;
    }
  }

  private static final class JwtVerifier {
    private final JwtAuthConfig config;
    private final JWKSource<SecurityContext> jwkSource;
    private final JWSVerifierFactory verifierFactory;
    private final byte[] sharedSecret;

    private JwtVerifier(JwtAuthConfig config, JWKSource<SecurityContext> jwkSource, byte[] sharedSecret) {
      this.config = config;
      this.jwkSource = jwkSource;
      this.sharedSecret = sharedSecret;
      this.verifierFactory = new DefaultJWSVerifierFactory();
    }

    private byte[] getSharedSecretBytes() {
      return sharedSecret;
    }

    private static JwtVerifier fromConfig(JwtAuthConfig config) {
      if (config == null) {
        return null;
      }
      byte[] sharedSecret = resolveSharedSecret(config);
      JWKSource<SecurityContext> jwkSource = null;
      if (config.getJwksUrl() != null && !config.getJwksUrl().isBlank()) {
        try {
          DefaultResourceRetriever retriever = new DefaultResourceRetriever(2000, 5000);
          DefaultJWKSetCache cache = buildJwkCache(config);
          jwkSource = new RemoteJWKSet<>(new java.net.URL(config.getJwksUrl()), retriever, cache);
        } catch (Exception e) {
          LOGGER.error("Failed to initialize JWKS URL {}", config.getJwksUrl(), e);
        }
      }
      return new JwtVerifier(config, jwkSource, sharedSecret);
    }

    private static DefaultJWKSetCache buildJwkCache(JwtAuthConfig config) {
      long ttl = config.getJwksCacheSeconds() == null ? 300 : config.getJwksCacheSeconds();
      return new DefaultJWKSetCache(ttl, ttl, TimeUnit.SECONDS);
    }

    private static byte[] resolveSharedSecret(JwtAuthConfig config) {
      if (config.getSharedSecret() != null && !config.getSharedSecret().isBlank()) {
        return config.getSharedSecret().getBytes(StandardCharsets.UTF_8);
      }
      if (config.getSharedSecretEnv() != null && !config.getSharedSecretEnv().isBlank()) {
        String value = System.getenv(config.getSharedSecretEnv());
        if (value != null && !value.isBlank()) {
          return value.getBytes(StandardCharsets.UTF_8);
        }
      }
      return null;
    }

    private JwtVerificationResult verify(String token) {
      try {
        SignedJWT signedJWT = SignedJWT.parse(token);
        JWSHeader header = signedJWT.getHeader();
        boolean verified = false;
        if (header.getAlgorithm().getName().startsWith("HS")) {
          if (sharedSecret == null) {
            return JwtVerificationResult.invalid("missing shared secret for HMAC token");
          }
          JWSVerifier verifier = new MACVerifier(sharedSecret);
          verified = signedJWT.verify(verifier);
        } else if (jwkSource != null) {
          List<JWK> keys = selectKeys(header);
          for (JWK jwk : keys) {
            if (jwk.getKeyUse() != null && jwk.getKeyUse() != KeyUse.SIGNATURE) {
              continue;
            }
            PublicKey publicKey = toPublicKey(jwk);
            if (publicKey == null) {
              continue;
            }
            JWSVerifier verifier = verifierFactory.createJWSVerifier(header, publicKey);
            if (signedJWT.verify(verifier)) {
              verified = true;
              break;
            }
          }
        }
        if (!verified) {
          return JwtVerificationResult.invalid("signature verification failed");
        }
        JWTClaimsSet claims = signedJWT.getJWTClaimsSet();
        String validationError = validateClaims(claims);
        if (validationError != null) {
          return JwtVerificationResult.invalid(validationError);
        }
        return JwtVerificationResult.valid(buildAuthContext(claims));
      } catch (Exception e) {
        return JwtVerificationResult.invalid(e.getMessage());
      }
    }

    private List<JWK> selectKeys(JWSHeader header) throws Exception {
      if (jwkSource == null) {
        return Collections.emptyList();
      }
      JWKMatcher.Builder matcher = new JWKMatcher.Builder()
        .keyUse(KeyUse.SIGNATURE)
        .keyID(header.getKeyID());
      JWKSelector selector = new JWKSelector(matcher.build());
      return jwkSource.get(selector, null);
    }

    private PublicKey toPublicKey(JWK jwk) throws com.nimbusds.jose.JOSEException {
      if (jwk == null) {
        return null;
      }
      JWK publicJwk = jwk.toPublicJWK();
      if (publicJwk instanceof RSAKey) {
        return ((RSAKey) publicJwk).toRSAPublicKey();
      }
      if (publicJwk instanceof ECKey) {
        return ((ECKey) publicJwk).toECPublicKey();
      }
      if (publicJwk instanceof OctetKeyPair) {
        return ((OctetKeyPair) publicJwk).toPublicKey();
      }
      return null;
    }

    private String validateClaims(JWTClaimsSet claims) {
      if (claims == null) {
        return "missing claims";
      }
      long skewSeconds = config.getClockSkewSeconds() == null ? 60 : config.getClockSkewSeconds();
      java.util.Date now = new java.util.Date();
      if (claims.getExpirationTime() != null
        && now.after(new java.util.Date(claims.getExpirationTime().getTime() + skewSeconds * 1000))) {
        return "token expired";
      }
      if (claims.getNotBeforeTime() != null
        && now.before(new java.util.Date(claims.getNotBeforeTime().getTime() - skewSeconds * 1000))) {
        return "token not yet valid";
      }
      if (config.getIssuer() != null && !config.getIssuer().isBlank()) {
        if (!config.getIssuer().equals(claims.getIssuer())) {
          return "issuer mismatch";
        }
      }
      if (config.getAudiences() != null && !config.getAudiences().isEmpty()) {
        List<String> aud = claims.getAudience();
        if (aud == null || aud.isEmpty()) {
          return "missing audience";
        }
        boolean match = false;
        for (String expected : config.getAudiences()) {
          if (aud.contains(expected)) {
            match = true;
            break;
          }
        }
        if (!match) {
          return "audience mismatch";
        }
      }
      return null;
    }

    private AuthContext buildAuthContext(JWTClaimsSet claims) {
      Map<String, Object> claimMap = claims.getClaims();
      String subject = claims.getSubject();
      Set<String> roles = extractRoles(claimMap);
      Set<String> scopes = extractScopes(claimMap);
      return new AuthContext(subject, roles, scopes, claimMap);
    }

    private Set<String> extractRoles(Map<String, Object> claims) {
      Set<String> roles = new LinkedHashSet<>();
      if (claims == null) {
        return roles;
      }
      Object rawRoles = claims.get("roles");
      roles.addAll(toStringSet(rawRoles));
      Object realmAccess = claims.get("realm_access");
      if (realmAccess instanceof Map) {
        Object realmRoles = ((Map<?, ?>) realmAccess).get("roles");
        roles.addAll(toStringSet(realmRoles));
      }
      Object resourceAccess = claims.get("resource_access");
      if (resourceAccess instanceof Map) {
        Map<?, ?> resourceMap = (Map<?, ?>) resourceAccess;
        for (Object entry : resourceMap.values()) {
          if (entry instanceof Map) {
            Object resRoles = ((Map<?, ?>) entry).get("roles");
            roles.addAll(toStringSet(resRoles));
          }
        }
      }
      return roles;
    }

    private Set<String> extractScopes(Map<String, Object> claims) {
      Set<String> scopes = new LinkedHashSet<>();
      if (claims == null) {
        return scopes;
      }
      Object scopeClaim = claims.get("scope");
      if (scopeClaim instanceof String) {
        scopes.addAll(Arrays.asList(((String) scopeClaim).split(" ")));
      } else {
        scopes.addAll(toStringSet(scopeClaim));
      }
      Object scp = claims.get("scp");
      scopes.addAll(toStringSet(scp));
      scopes.removeIf(value -> value == null || value.isBlank());
      return scopes;
    }

    private Set<String> toStringSet(Object raw) {
      Set<String> values = new LinkedHashSet<>();
      if (raw == null) {
        return values;
      }
      if (raw instanceof List) {
        for (Object value : (List<?>) raw) {
          if (value != null) {
            String str = value.toString();
            if (!str.isBlank()) {
              values.add(str);
            }
          }
        }
        return values;
      }
      if (raw instanceof String) {
        String str = ((String) raw).trim();
        if (!str.isBlank()) {
          values.add(str);
        }
      }
      return values;
    }

    private static final class JwtVerificationResult {
      private final boolean valid;
      private final String message;
      private final AuthContext context;

      private JwtVerificationResult(boolean valid, String message, AuthContext context) {
        this.valid = valid;
        this.message = message;
        this.context = context;
      }

      private static JwtVerificationResult valid(AuthContext context) {
        return new JwtVerificationResult(true, "ok", context);
      }

      private static JwtVerificationResult invalid(String message) {
        return new JwtVerificationResult(false, message, null);
      }

      private boolean isValid() {
        return valid;
      }

      private String getMessage() {
        return message;
      }

      private AuthContext getContext() {
        return context;
      }
    }
  }

  private static boolean hasSubscriptions(List<KafkaSubscriptionConfig> subscriptions) {
    if (subscriptions == null) {
      return false;
    }
    return subscriptions.stream().anyMatch(sub ->
      sub != null && sub.getFieldName() != null && !sub.getFieldName().isBlank());
  }

  private static boolean hasPostgresSubscriptions(List<PostgresSubscriptionConfig> subscriptions) {
    if (subscriptions == null) {
      return false;
    }
    return subscriptions.stream().anyMatch(sub ->
      sub != null && sub.getFieldName() != null && !sub.getFieldName().isBlank());
  }

  private static CompletionStage<Object> executeQuery(Vertx vertx,
                                                      Map<String, PgPool> pools,
                                                      Map<String, DuckDbConnectionConfig> duckdbConnections,
                                                      Map<String, JdbcConnectionConfig> jdbcConnections,
                                                      Map<String, MongoConnectionConfig> mongoConnections,
                                                      Map<String, MongoClient> mongoClients,
                                                      Map<String, ElasticsearchConnectionConfig> elasticsearchConnections,
                                                      Map<String, RestClient> elasticsearchClients,
                                                      Map<String, DynamoDbConnectionConfig> dynamodbConnections,
                                                      Map<String, DynamoDbClient> dynamodbClients,
                                                      QueryConfig query,
                                                      graphql.schema.DataFetchingEnvironment env) {
    AuthorizationDecision decision = authorizeQuery(query, env);
    if (!decision.isAllowed()) {
      CompletableFuture<Object> failed = new CompletableFuture<>();
      failed.completeExceptionally(new IllegalStateException(decision.getMessage()));
      return failed;
    }
    QueryConfig.Engine engine = query.getEngine();
    if (engine == QueryConfig.Engine.DUCKDB) {
      return executeDuckDbQuery(vertx, duckdbConnections, query, env);
    }
    if (engine == QueryConfig.Engine.JDBC) {
      return executeJdbcQuery(vertx, jdbcConnections, query, env);
    }
    if (engine == QueryConfig.Engine.MONGODB) {
      return executeMongoQuery(vertx, mongoConnections, mongoClients, query, env);
    }
    if (engine == QueryConfig.Engine.ELASTICSEARCH) {
      return executeElasticsearchQuery(vertx, elasticsearchConnections, elasticsearchClients, query, env);
    }
    if (engine == QueryConfig.Engine.DYNAMODB) {
      return executeDynamoDbQuery(vertx, dynamodbConnections, dynamodbClients, query, env);
    }
    return executePostgresQuery(vertx, pools, query, env);
  }

  private static AuthorizationDecision authorizeQuery(QueryConfig query,
                                                      graphql.schema.DataFetchingEnvironment env) {
    return authorizeOperation(
      "query",
      query.getName(),
      query.getRequiredRoles(),
      query.getRequiredScopes(),
      env);
  }

  private static AuthorizationDecision authorizeOperation(String kind,
                                                          String name,
                                                          List<String> requiredRoles,
                                                          List<String> requiredScopes,
                                                          graphql.schema.DataFetchingEnvironment env) {
    List<String> normalizedRoles = normalizeAuthList(requiredRoles);
    List<String> normalizedScopes = normalizeAuthList(requiredScopes);
    if (normalizedRoles.isEmpty() && normalizedScopes.isEmpty()) {
      return AuthorizationDecision.allow();
    }
    LOGGER.info("Authz check {}={} roles={} scopes={}", kind, name, normalizedRoles, normalizedScopes);
    AuthContext authContext = getAuthContext(env);
    if (authContext == null || !authContext.isAuthenticated()) {
      LOGGER.warn("Authz denied: no authenticated context for {}={}", kind, name);
      return AuthorizationDecision.deny("Unauthorized");
    }
    if (!normalizedRoles.isEmpty() && !authContext.hasAnyRole(normalizedRoles)) {
      LOGGER.warn("Authz denied: missing role for {}={} roles={} userRoles={}",
        kind, name, normalizedRoles, authContext.roles);
      return AuthorizationDecision.deny("Forbidden: missing role");
    }
    if (!normalizedScopes.isEmpty() && !authContext.hasAnyScope(normalizedScopes)) {
      LOGGER.warn("Authz denied: missing scope for {}={} scopes={} userScopes={}",
        kind, name, normalizedScopes, authContext.scopes);
      return AuthorizationDecision.deny("Forbidden: missing scope");
    }
    LOGGER.info("Authz allowed for {}={}", kind, name);
    return AuthorizationDecision.allow();
  }

  private static List<String> normalizeAuthList(List<String> values) {
    if (values == null || values.isEmpty()) {
      return Collections.emptyList();
    }
    List<String> cleaned = new ArrayList<>();
    for (String value : values) {
      if (value == null) {
        continue;
      }
      String trimmed = value.trim();
      if (!trimmed.isEmpty()) {
        cleaned.add(trimmed);
      }
    }
    return cleaned;
  }

  private static AuthContext getAuthContext(graphql.schema.DataFetchingEnvironment env) {
    if (env == null) {
      return null;
    }
    GraphQLContext context = env.getGraphQlContext();
    if (context == null) {
      Object legacy = env.getContext();
      return extractAuthFromContext(legacy);
    }
    Object auth = context.get("auth");
    if (auth instanceof AuthContext) {
      return (AuthContext) auth;
    }
    Object legacy = env.getContext();
    return extractAuthFromContext(legacy);
  }

  private static AuthContext extractAuthFromContext(Object legacy) {
    if (legacy == null) {
      return null;
    }
    if (legacy instanceof AuthContext) {
      return (AuthContext) legacy;
    }
    if (legacy instanceof Map) {
      Object auth = ((Map<?, ?>) legacy).get("auth");
      if (auth instanceof AuthContext) {
        return (AuthContext) auth;
      }
    }
    return null;
  }

  private static CompletionStage<Object> executePostgresQuery(Vertx vertx,
                                                              Map<String, PgPool> pools,
                                                              QueryConfig query,
                                                              graphql.schema.DataFetchingEnvironment env) {
    CompletableFuture<Object> future = new CompletableFuture<>();
    PgPool pool = selectPool(query, pools);
    if (pool == null) {
      future.completeExceptionally(new IllegalStateException("No Postgres pool available"));
      return future;
    }
    PaginationContext paginationContext = buildPaginationContext(query, env);
    attachPaginationContext(env, paginationContext);
    if (query.getParams() == null || query.getParams().isEmpty()) {
      pool.query(query.getSql()).execute(ar -> handleResult(ar, query, paginationContext, future));
      return future;
    }

    CompletionStage<List<Object>> valuesStage = resolveQueryValuesAsync(vertx, query, env);
    toCompletableFuture(valuesStage).whenComplete((values, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
        return;
      }
      Tuple tuple = Tuple.tuple(values);
      Integer timeoutMs = query.getTimeoutMs();
      if (timeoutMs != null && timeoutMs > 0) {
        executePgPreparedWithStatementTimeout(pool, query.getSql(), tuple, timeoutMs, ar ->
          handleResult(ar, query, paginationContext, future));
      } else {
        pool.preparedQuery(query.getSql()).execute(tuple, ar -> handleResult(ar, query, paginationContext, future));
      }
    });
    return future;
  }

  private static CompletionStage<Object> executeDuckDbQuery(Vertx vertx,
                                                            Map<String, DuckDbConnectionConfig> duckdbConnections,
                                                            QueryConfig query,
                                                            graphql.schema.DataFetchingEnvironment env) {
    CompletableFuture<Object> future = new CompletableFuture<>();
    DuckDbConnectionConfig connection = selectDuckDbConnection(query, duckdbConnections);
    if (connection == null) {
      future.completeExceptionally(new IllegalStateException("No DuckDB connection available"));
      return future;
    }

    PaginationContext paginationContext = buildPaginationContext(query, env);
    attachPaginationContext(env, paginationContext);
    CompletionStage<List<Object>> valuesStage = resolveQueryValuesAsync(vertx, query, env);
    toCompletableFuture(valuesStage).whenComplete((values, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
        return;
      }
      vertx.executeBlocking(promise -> {
        try (Connection db = openDuckDbConnection(connection)) {
          Object result = executeDuckDbSql(db, query, values);
          promise.complete(applyPaginationResult(query, paginationContext, result));
        } catch (Exception e) {
          promise.fail(e);
        }
      }, ar -> {
        if (ar.failed()) {
          future.completeExceptionally(ar.cause());
        } else {
          future.complete(ar.result());
        }
      });
    });
    return future;
  }

  private static CompletionStage<Object> executeJdbcQuery(Vertx vertx,
                                                          Map<String, JdbcConnectionConfig> jdbcConnections,
                                                          QueryConfig query,
                                                          graphql.schema.DataFetchingEnvironment env) {
    CompletableFuture<Object> future = new CompletableFuture<>();
    JdbcConnectionConfig connection = selectJdbcConnection(query, jdbcConnections);
    if (connection == null) {
      future.completeExceptionally(new IllegalStateException("No JDBC connection available"));
      return future;
    }
    PaginationContext paginationContext = buildPaginationContext(query, env);
    attachPaginationContext(env, paginationContext);
    CompletionStage<List<Object>> valuesStage = resolveQueryValuesAsync(vertx, query, env);
    toCompletableFuture(valuesStage).whenComplete((values, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
        return;
      }
      vertx.executeBlocking(promise -> {
        try (Connection db = openJdbcConnection(connection)) {
          Object result = executeJdbcSql(db, query, values);
          promise.complete(applyPaginationResult(query, paginationContext, result));
        } catch (Exception e) {
          promise.fail(e);
        }
      }, ar -> {
        if (ar.failed()) {
          future.completeExceptionally(ar.cause());
        } else {
          future.complete(ar.result());
        }
      });
    });
    return future;
  }

  private static CompletionStage<Object> executeMongoQuery(Vertx vertx,
                                                           Map<String, MongoConnectionConfig> mongoConnections,
                                                           Map<String, MongoClient> mongoClients,
                                                           QueryConfig query,
                                                           graphql.schema.DataFetchingEnvironment env) {
    CompletableFuture<Object> future = new CompletableFuture<>();
    MongoConnectionConfig connection = selectMongoConnection(query, mongoConnections);
    if (connection == null) {
      future.completeExceptionally(new IllegalStateException("No MongoDB connection available"));
      return future;
    }
    MongoClient client = mongoClients.get(connection.getName());
    if (client == null) {
      future.completeExceptionally(new IllegalStateException("MongoDB client missing for connection "
        + connection.getName()));
      return future;
    }
    PaginationContext paginationContext = buildPaginationContext(query, env);
    attachPaginationContext(env, paginationContext);
    Map<String, Object> paramValues = buildParamValueMap(query, env);
    vertx.executeBlocking(promise -> {
      try {
        if (connection.getDatabase() == null || connection.getDatabase().isBlank()) {
          throw new IllegalStateException("MongoDB connection " + connection.getName() + " missing database");
        }
        MongoDatabase database = client.getDatabase(connection.getDatabase());
        MongoQueryConfig mongo = query.getMongo();
        if (mongo == null || mongo.getCollection() == null || mongo.getCollection().isBlank()) {
          throw new IllegalStateException("Mongo query missing collection for " + query.getName());
        }
        MongoCollection<Document> collection = database.getCollection(mongo.getCollection());
        List<Map<String, Object>> results = new ArrayList<>();
        String pipelineJson = applyJsonTemplate(mongo.getPipeline(), paramValues);
        if (pipelineJson == null || pipelineJson.isBlank()) {
          for (Document doc : collection.find()) {
            results.add(new HashMap<>(doc));
          }
        } else {
          List<Bson> pipeline = parseMongoPipeline(pipelineJson);
          for (Document doc : collection.aggregate(pipeline)) {
            results.add(new HashMap<>(doc));
          }
        }
        Object result = query.getResultMode() == QueryConfig.ResultMode.SINGLE
          ? (results.isEmpty() ? null : results.get(0))
          : results;
        promise.complete(applyPaginationResult(query, paginationContext, result));
      } catch (Exception e) {
        promise.fail(e);
      }
    }, ar -> {
      if (ar.failed()) {
        future.completeExceptionally(ar.cause());
      } else {
        future.complete(ar.result());
      }
    });
    return future;
  }

  private static CompletionStage<Object> executeElasticsearchQuery(
    Vertx vertx,
    Map<String, ElasticsearchConnectionConfig> elasticsearchConnections,
    Map<String, RestClient> elasticsearchClients,
    QueryConfig query,
    graphql.schema.DataFetchingEnvironment env) {
    CompletableFuture<Object> future = new CompletableFuture<>();
    ElasticsearchConnectionConfig connection = selectElasticsearchConnection(query, elasticsearchConnections);
    if (connection == null) {
      future.completeExceptionally(new IllegalStateException("No Elasticsearch connection available"));
      return future;
    }
    RestClient client = elasticsearchClients.get(connection.getName());
    if (client == null) {
      future.completeExceptionally(new IllegalStateException("Elasticsearch client missing for connection "
        + connection.getName()));
      return future;
    }
    PaginationContext paginationContext = buildPaginationContext(query, env);
    attachPaginationContext(env, paginationContext);
    Map<String, Object> paramValues = buildParamValueMap(query, env);
    vertx.executeBlocking(promise -> {
      try {
        ElasticsearchQueryConfig esQuery = query.getElasticsearch();
        if (esQuery == null || esQuery.getIndex() == null || esQuery.getIndex().isBlank()) {
          throw new IllegalStateException("Elasticsearch query missing index for " + query.getName());
        }
        String queryBody = applyJsonTemplate(esQuery.getQuery(), paramValues);
        if (queryBody == null || queryBody.isBlank()) {
          queryBody = "{\"query\":{\"match_all\":{}}}";
        }
        Request request = new Request("POST", "/" + esQuery.getIndex() + "/_search");
        request.setJsonEntity(queryBody);
        Response response = client.performRequest(request);
        String payload = EntityUtils.toString(response.getEntity());
        JsonNode root = JSON_MAPPER.readTree(payload);
        JsonNode hitsNode = root.path("hits").path("hits");
        List<Map<String, Object>> results = new ArrayList<>();
        if (hitsNode.isArray()) {
          for (JsonNode hit : hitsNode) {
            Map<String, Object> doc;
            JsonNode source = hit.get("_source");
            if (source != null && !source.isNull()) {
              doc = JSON_MAPPER.convertValue(source, Map.class);
            } else {
              doc = JSON_MAPPER.convertValue(hit, Map.class);
            }
            JsonNode id = hit.get("_id");
            if (id != null && !id.isNull()) {
              doc.put("_id", id.asText());
            }
            results.add(doc);
          }
        }
        Object result = query.getResultMode() == QueryConfig.ResultMode.SINGLE
          ? (results.isEmpty() ? null : results.get(0))
          : results;
        promise.complete(applyPaginationResult(query, paginationContext, result));
      } catch (Exception e) {
        promise.fail(e);
      }
    }, ar -> {
      if (ar.failed()) {
        future.completeExceptionally(ar.cause());
      } else {
        future.complete(ar.result());
      }
    });
    return future;
  }

  private static CompletionStage<Object> executeDynamoDbQuery(
    Vertx vertx,
    Map<String, DynamoDbConnectionConfig> dynamodbConnections,
    Map<String, DynamoDbClient> dynamodbClients,
    QueryConfig query,
    graphql.schema.DataFetchingEnvironment env) {
    CompletableFuture<Object> future = new CompletableFuture<>();
    DynamoDbConnectionConfig connection = selectDynamoDbConnection(query, dynamodbConnections);
    if (connection == null) {
      future.completeExceptionally(new IllegalStateException("No DynamoDB connection available"));
      return future;
    }
    DynamoDbClient client = dynamodbClients.get(connection.getName());
    if (client == null) {
      future.completeExceptionally(new IllegalStateException("DynamoDB client missing for connection "
        + connection.getName()));
      return future;
    }
    PaginationContext paginationContext = buildPaginationContext(query, env);
    attachPaginationContext(env, paginationContext);
    Map<String, Object> paramValues = buildParamValueMap(query, env);
    vertx.executeBlocking(promise -> {
      try {
        DynamoDbQueryConfig dynamo = query.getDynamodb();
        if (dynamo == null || dynamo.getTable() == null || dynamo.getTable().isBlank()) {
          throw new IllegalStateException("DynamoDB query missing table for " + query.getName());
        }
        if (dynamo.getKeyConditionExpression() == null || dynamo.getKeyConditionExpression().isBlank()) {
          throw new IllegalStateException("DynamoDB query missing keyConditionExpression for " + query.getName());
        }
        QueryRequest.Builder builder = QueryRequest.builder()
          .tableName(dynamo.getTable())
          .keyConditionExpression(dynamo.getKeyConditionExpression());
        if (dynamo.getIndexName() != null && !dynamo.getIndexName().isBlank()) {
          builder.indexName(dynamo.getIndexName());
        }
        if (dynamo.getLimit() != null && dynamo.getLimit() > 0) {
          builder.limit(dynamo.getLimit());
        }
        if (dynamo.getExpressionAttributeNames() != null && !dynamo.getExpressionAttributeNames().isEmpty()) {
          builder.expressionAttributeNames(dynamo.getExpressionAttributeNames());
        }
        Map<String, Object> rawValues = dynamo.getExpressionAttributeValues();
        if (rawValues != null && !rawValues.isEmpty()) {
          builder.expressionAttributeValues(buildDynamoAttributeValues(rawValues, paramValues));
        }
        QueryResponse response = client.query(builder.build());
        List<Map<String, Object>> results = new ArrayList<>();
        for (Map<String, AttributeValue> item : response.items()) {
          results.add(convertDynamoItem(item));
        }
        Object result = query.getResultMode() == QueryConfig.ResultMode.SINGLE
          ? (results.isEmpty() ? null : results.get(0))
          : results;
        promise.complete(applyPaginationResult(query, paginationContext, result));
      } catch (Exception e) {
        promise.fail(e);
      }
    }, ar -> {
      if (ar.failed()) {
        future.completeExceptionally(ar.cause());
      } else {
        future.complete(ar.result());
      }
    });
    return future;
  }

  private static void handleResult(io.vertx.core.AsyncResult<RowSet<Row>> ar,
                                   QueryConfig query,
                                   PaginationContext paginationContext,
                                   CompletableFuture<Object> future) {
    if (ar.failed()) {
      future.completeExceptionally(ar.cause());
      return;
    }

    RowSet<Row> rows = ar.result();
    if (query.getResultMode() == QueryConfig.ResultMode.SINGLE) {
      Row row = rows.iterator().hasNext() ? rows.iterator().next() : null;
      if (row == null) {
        future.complete(null);
      } else {
        future.complete(row.toJson().getMap());
      }
    } else {
      List<Map<String, Object>> results = new ArrayList<>();
      for (Row row : rows) {
        results.add(row.toJson().getMap());
      }
      future.complete(applyPaginationResult(query, paginationContext, results));
    }
  }

  private static PaginationContext buildPaginationContext(QueryConfig query,
                                                          graphql.schema.DataFetchingEnvironment env) {
    PaginationConfig pagination = query == null ? null : query.getPagination();
    if (pagination == null) {
      return null;
    }
    if (query.getResultMode() == QueryConfig.ResultMode.SINGLE) {
      throw new IllegalStateException("Pagination requires LIST resultMode for query " + query.getName());
    }
    CursorSigner signer = PAGINATION_SIGNERS.get(query.getName());
    if (signer == null) {
      throw new IllegalStateException("Pagination signer missing for query " + query.getName());
    }

    String firstArg = pagination.getFirstArg();
    if (firstArg == null || firstArg.isBlank()) {
      firstArg = "first";
    }
    Integer requested = parseIntArgument(env.getArgument(firstArg));
    if (requested == null) {
      throw new IllegalStateException("Pagination requires '" + firstArg + "' argument for query " + query.getName());
    }
    int pageSize = requested;
    Integer maxPageSize = pagination.getMaxPageSize();
    if (maxPageSize != null && maxPageSize > 0 && pageSize > maxPageSize) {
      pageSize = maxPageSize;
    }
    if (pageSize <= 0) {
      throw new IllegalStateException("Pagination page size must be > 0 for query " + query.getName());
    }
    boolean fetchExtra = !Boolean.FALSE.equals(pagination.getFetchExtra());
    int sqlLimit = fetchExtra ? pageSize + 1 : pageSize;

    String afterArg = pagination.getAfterArg();
    if (afterArg == null || afterArg.isBlank()) {
      afterArg = "after";
    }
    Object afterValue = env.getArgument(afterArg);
    String afterToken = afterValue == null ? null : String.valueOf(afterValue);
    Map<String, Object> afterPayload = null;
    if (afterToken != null && !afterToken.isBlank()) {
      afterPayload = signer.decode(afterToken);
      if (afterPayload == null) {
        throw new IllegalStateException("Invalid cursor for query " + query.getName());
      }
    }

    return new PaginationContext(
      pagination,
      signer,
      firstArg,
      afterArg,
      pageSize,
      sqlLimit,
      afterPayload);
  }

  private static void attachPaginationContext(graphql.schema.DataFetchingEnvironment env,
                                              PaginationContext context) {
    if (context == null || env == null) {
      return;
    }
    GraphQLContext ctx = env.getGraphQlContext();
    if (ctx != null) {
      ctx.put(PAGINATION_CONTEXT_KEY, context);
    }
  }

  private static Object applyPaginationResult(QueryConfig query,
                                              PaginationContext paginationContext,
                                              Object result) {
    if (paginationContext == null || result == null) {
      return result;
    }
    if (!(result instanceof List)) {
      return result;
    }
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> rows = (List<Map<String, Object>>) result;
    int pageSize = paginationContext.pageSize;
    boolean fetchExtra = paginationContext.fetchExtra;
    boolean hasNextPage = false;
    if (fetchExtra && rows.size() > pageSize) {
      hasNextPage = true;
      rows = new ArrayList<>(rows.subList(0, pageSize));
    }

    PaginationConfig pagination = paginationContext.pagination;
    String edgesField = defaultString(pagination.getEdgesFieldName(), "edges");
    String nodeField = defaultString(pagination.getNodeFieldName(), "node");
    String cursorField = defaultString(pagination.getCursorFieldName(), "cursor");
    String pageInfoField = defaultString(pagination.getPageInfoFieldName(), "pageInfo");
    String endCursorField = defaultString(pagination.getEndCursorFieldName(), "endCursor");
    String hasNextField = defaultString(pagination.getHasNextPageFieldName(), "hasNextPage");

    List<Map<String, Object>> edges = new ArrayList<>();
    String endCursor = null;
    for (Map<String, Object> row : rows) {
      if (row == null) {
        continue;
      }
      String cursor = paginationContext.signer.encode(buildCursorPayload(pagination, row));
      Map<String, Object> edge = new HashMap<>();
      edge.put(cursorField, cursor);
      edge.put(nodeField, row);
      edges.add(edge);
      endCursor = cursor;
    }

    Map<String, Object> pageInfo = new HashMap<>();
    pageInfo.put(hasNextField, hasNextPage);
    pageInfo.put(endCursorField, endCursor);

    Map<String, Object> connection = new HashMap<>();
    connection.put(edgesField, edges);
    connection.put(pageInfoField, pageInfo);
    return connection;
  }

  private static Map<String, Object> buildCursorPayload(PaginationConfig pagination, Map<String, Object> row) {
    Map<String, Object> payload = new HashMap<>();
    if (pagination.getCursorFields() == null) {
      return payload;
    }
    for (String field : pagination.getCursorFields()) {
      if (field == null || field.isBlank()) {
        continue;
      }
      if (!row.containsKey(field)) {
        throw new IllegalStateException("Cursor field '" + field + "' missing in query result");
      }
      payload.put(field, row.get(field));
    }
    return payload;
  }

  private static Integer parseIntArgument(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Number) {
      return ((Number) value).intValue();
    }
    try {
      return Integer.parseInt(String.valueOf(value));
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static String defaultString(String value, String fallback) {
    return (value == null || value.isBlank()) ? fallback : value;
  }

  private static void handleMutationResult(io.vertx.core.AsyncResult<RowSet<Row>> ar,
                                           CompletableFuture<Object> future,
                                           Object enrichedPayload,
                                           boolean hasPayloadArg) {
    if (ar.failed()) {
      future.completeExceptionally(ar.cause());
      return;
    }
    RowSet<Row> rows = ar.result();
    if (rows == null || !rows.iterator().hasNext()) {
      if (hasPayloadArg) {
        future.complete(enrichedPayload);
      } else {
        future.complete(rows == null ? 0 : rows.rowCount());
      }
      return;
    }
    List<Map<String, Object>> results = new ArrayList<>();
    for (Row row : rows) {
      results.add(row.toJson().getMap());
    }
    if (hasPayloadArg) {
      Object decorated = decorateMutationResults(results, enrichedPayload);
      future.complete(decorated);
      return;
    }
    if (results.size() == 1) {
      future.complete(results.get(0));
    } else {
      future.complete(results);
    }
  }

  private static Object decorateMutationResults(List<Map<String, Object>> results, Object enrichedPayload) {
    Map<String, Object> payloadMap = toPayloadMap(enrichedPayload);
    if (payloadMap == null) {
      if (results.size() == 1) {
        return results.get(0);
      }
      return results;
    }
    if (results.isEmpty()) {
      return payloadMap;
    }
    if (results.size() == 1) {
      return mergePayload(payloadMap, results.get(0));
    }
    List<Map<String, Object>> merged = new ArrayList<>(results.size());
    for (Map<String, Object> row : results) {
      merged.add(mergePayload(payloadMap, row));
    }
    return merged;
  }

  private static Map<String, Object> mergePayload(Map<String, Object> payload, Map<String, Object> row) {
    Map<String, Object> merged = new LinkedHashMap<>();
    merged.putAll(payload);
    if (row != null) {
      merged.putAll(row);
    }
    return merged;
  }

  private static Map<String, Object> toPayloadMap(Object payloadArg) {
    JsonObject payload = toJsonObject(payloadArg);
    if (payload == null) {
      return null;
    }
    return payload.getMap();
  }

  private static Object executeDuckDbSql(Connection db,
                                         QueryConfig query,
                                         List<Object> values) throws SQLException {
    if (values == null || values.isEmpty()) {
      try (Statement statement = db.createStatement();
           ResultSet rs = statement.executeQuery(query.getSql())) {
        return mapDuckDbResult(rs, query);
      }
    }
    try (PreparedStatement statement = db.prepareStatement(query.getSql())) {
      for (int i = 0; i < values.size(); i++) {
        statement.setObject(i + 1, values.get(i));
      }
      try (ResultSet rs = statement.executeQuery()) {
        return mapDuckDbResult(rs, query);
      }
    }
  }

  private static Object executeJdbcSql(Connection db,
                                       QueryConfig query,
                                       List<Object> values) throws SQLException {
    String sql = query.getSql();
    if (sql == null || sql.isBlank()) {
      throw new IllegalStateException("JDBC query missing sql for " + query.getName());
    }
    if (values == null || values.isEmpty()) {
      try (Statement statement = db.createStatement();
           ResultSet rs = statement.executeQuery(sql)) {
        return mapJdbcResult(rs, query);
      }
    }
    try (PreparedStatement statement = db.prepareStatement(sql)) {
      for (int i = 0; i < values.size(); i++) {
        statement.setObject(i + 1, values.get(i));
      }
      try (ResultSet rs = statement.executeQuery()) {
        return mapJdbcResult(rs, query);
      }
    }
  }

  private static Object mapDuckDbResult(ResultSet rs, QueryConfig query) throws SQLException {
    ResultSetMetaData meta = rs.getMetaData();
    int columnCount = meta.getColumnCount();
    if (query.getResultMode() == QueryConfig.ResultMode.SINGLE) {
      if (!rs.next()) {
        return null;
      }
      return rowFromResultSet(rs, meta, columnCount);
    }
    List<Map<String, Object>> results = new ArrayList<>();
    while (rs.next()) {
      results.add(rowFromResultSet(rs, meta, columnCount));
    }
    return results;
  }

  private static Object mapJdbcResult(ResultSet rs, QueryConfig query) throws SQLException {
    ResultSetMetaData meta = rs.getMetaData();
    int columnCount = meta.getColumnCount();
    if (query.getResultMode() == QueryConfig.ResultMode.SINGLE) {
      if (!rs.next()) {
        return null;
      }
      return rowFromResultSet(rs, meta, columnCount);
    }
    List<Map<String, Object>> results = new ArrayList<>();
    while (rs.next()) {
      results.add(rowFromResultSet(rs, meta, columnCount));
    }
    return results;
  }

  private static Map<String, Object> rowFromResultSet(ResultSet rs,
                                                      ResultSetMetaData meta,
                                                      int columnCount) throws SQLException {
    Map<String, Object> row = new HashMap<>();
    for (int i = 1; i <= columnCount; i++) {
      String name = meta.getColumnLabel(i);
      if (name == null || name.isBlank()) {
        name = meta.getColumnName(i);
      }
      Object value = rs.getObject(i);
      row.put(name, normalizeJdbcValue(value));
    }
    return row;
  }

  private static Object normalizeJdbcValue(Object value) throws SQLException {
    if (value instanceof java.sql.Array) {
      Object array = ((java.sql.Array) value).getArray();
      if (array instanceof Object[]) {
        Object[] values = (Object[]) array;
        List<Object> list = new ArrayList<>(values.length);
        Collections.addAll(list, values);
        return list;
      }
      return array;
    }
    return value;
  }

  private static Connection openDuckDbConnection(DuckDbConnectionConfig connection) throws SQLException {
    String database = connection.getDatabase();
    String url = "jdbc:duckdb:" + (database == null ? "" : database);
    Connection db = DriverManager.getConnection(url);
    enableIcebergExtension(db);
    applyDuckDbSettings(db, connection.getSettings());
    return db;
  }

  private static Connection openJdbcConnection(JdbcConnectionConfig connection) throws SQLException {
    String url = connection.getJdbcUrl();
    if (url == null || url.isBlank()) {
      throw new IllegalStateException("JDBC connection " + connection.getName() + " missing jdbcUrl");
    }
    String driver = connection.getDriverClass();
    if (driver != null && !driver.isBlank()) {
      try {
        Class.forName(driver);
      } catch (ClassNotFoundException e) {
        throw new IllegalStateException("JDBC driver not found: " + driver, e);
      }
    }
    java.util.Properties props = new java.util.Properties();
    if (connection.getUser() != null && !connection.getUser().isBlank()) {
      props.setProperty("user", connection.getUser());
    }
    String password = resolveJdbcPassword(connection);
    if (password != null && !password.isBlank()) {
      props.setProperty("password", password);
    }
    return DriverManager.getConnection(url, props);
  }

  private static String resolveJdbcPassword(JdbcConnectionConfig connection) {
    String password = connection.getPassword();
    if (password == null || password.isBlank()) {
      String envName = connection.getPasswordEnv();
      if (envName != null && !envName.isBlank()) {
        password = System.getenv(envName);
      }
    }
    return password;
  }

  private static void enableIcebergExtension(Connection db) throws SQLException {
    try (Statement statement = db.createStatement()) {
      statement.execute("INSTALL iceberg");
    } catch (SQLException e) {
      LOGGER.warn("DuckDB iceberg extension install failed (continuing to load)", e);
    }
    try (Statement statement = db.createStatement()) {
      statement.execute("LOAD iceberg");
    }
  }

  private static void applyDuckDbSettings(Connection db, Map<String, String> settings) throws SQLException {
    if (settings == null || settings.isEmpty()) {
      return;
    }
    try (Statement statement = db.createStatement()) {
      for (Map.Entry<String, String> entry : settings.entrySet()) {
        String key = entry.getKey();
        if (key == null || key.isBlank()) {
          continue;
        }
        String value = entry.getValue();
        String literal = value == null ? "NULL" : "'" + escapeSqlLiteral(value) + "'";
        statement.execute("SET " + key + " = " + literal);
      }
    }
  }

  private static String escapeSqlLiteral(String value) {
    return value.replace("'", "''");
  }

  private static PgPool selectPool(QueryConfig query, Map<String, PgPool> pools) {
    if (pools.isEmpty()) {
      return null;
    }
    String connectionRef = query.getConnectionRef();
    if (connectionRef == null || connectionRef.isBlank()) {
      if (pools.size() == 1) {
        return pools.values().iterator().next();
      }
      throw new IllegalStateException("Query " + query.getName() + " missing connectionRef");
    }
    PgPool pool = pools.get(connectionRef);
    if (pool == null) {
      throw new IllegalStateException("Connection " + connectionRef + " not configured");
    }
    return pool;
  }

  private static PgPool selectPostgresPool(PostgresMutationConfig mutation, Map<String, PgPool> pools) {
    if (pools.isEmpty()) {
      return null;
    }
    String connectionRef = mutation.getConnectionRef();
    if (connectionRef == null || connectionRef.isBlank()) {
      if (pools.size() == 1) {
        return pools.values().iterator().next();
      }
      throw new IllegalStateException("Mutation " + mutation.getName() + " missing connectionRef");
    }
    PgPool pool = pools.get(connectionRef);
    if (pool == null) {
      throw new IllegalStateException("Connection " + connectionRef + " not configured");
    }
    return pool;
  }

  private static DuckDbConnectionConfig selectDuckDbConnection(QueryConfig query,
                                                               Map<String, DuckDbConnectionConfig> connections) {
    if (connections.isEmpty()) {
      return null;
    }
    String connectionRef = query.getConnectionRef();
    if (connectionRef == null || connectionRef.isBlank()) {
      if (connections.size() == 1) {
        return connections.values().iterator().next();
      }
      throw new IllegalStateException("Query " + query.getName() + " missing connectionRef");
    }
    DuckDbConnectionConfig connection = connections.get(connectionRef);
    if (connection == null) {
      throw new IllegalStateException("DuckDB connection " + connectionRef + " not configured");
    }
    return connection;
  }

  private static JdbcConnectionConfig selectJdbcConnection(QueryConfig query,
                                                           Map<String, JdbcConnectionConfig> connections) {
    if (connections.isEmpty()) {
      return null;
    }
    String connectionRef = query.getConnectionRef();
    if (connectionRef == null || connectionRef.isBlank()) {
      if (connections.size() == 1) {
        return connections.values().iterator().next();
      }
      throw new IllegalStateException("Query " + query.getName() + " missing connectionRef");
    }
    JdbcConnectionConfig connection = connections.get(connectionRef);
    if (connection == null) {
      throw new IllegalStateException("JDBC connection " + connectionRef + " not configured");
    }
    return connection;
  }

  private static MongoConnectionConfig selectMongoConnection(QueryConfig query,
                                                             Map<String, MongoConnectionConfig> connections) {
    if (connections.isEmpty()) {
      return null;
    }
    String connectionRef = query.getConnectionRef();
    if (connectionRef == null || connectionRef.isBlank()) {
      if (connections.size() == 1) {
        return connections.values().iterator().next();
      }
      throw new IllegalStateException("Query " + query.getName() + " missing connectionRef");
    }
    MongoConnectionConfig connection = connections.get(connectionRef);
    if (connection == null) {
      throw new IllegalStateException("MongoDB connection " + connectionRef + " not configured");
    }
    return connection;
  }

  private static ElasticsearchConnectionConfig selectElasticsearchConnection(
    QueryConfig query,
    Map<String, ElasticsearchConnectionConfig> connections) {
    if (connections.isEmpty()) {
      return null;
    }
    String connectionRef = query.getConnectionRef();
    if (connectionRef == null || connectionRef.isBlank()) {
      if (connections.size() == 1) {
        return connections.values().iterator().next();
      }
      throw new IllegalStateException("Query " + query.getName() + " missing connectionRef");
    }
    ElasticsearchConnectionConfig connection = connections.get(connectionRef);
    if (connection == null) {
      throw new IllegalStateException("Elasticsearch connection " + connectionRef + " not configured");
    }
    return connection;
  }

  private static DynamoDbConnectionConfig selectDynamoDbConnection(
    QueryConfig query,
    Map<String, DynamoDbConnectionConfig> connections) {
    if (connections.isEmpty()) {
      return null;
    }
    String connectionRef = query.getConnectionRef();
    if (connectionRef == null || connectionRef.isBlank()) {
      if (connections.size() == 1) {
        return connections.values().iterator().next();
      }
      throw new IllegalStateException("Query " + query.getName() + " missing connectionRef");
    }
    DynamoDbConnectionConfig connection = connections.get(connectionRef);
    if (connection == null) {
      throw new IllegalStateException("DynamoDB connection " + connectionRef + " not configured");
    }
    return connection;
  }

  private static List<Object> resolveQueryValues(QueryConfig query, DataFetchingEnvironment env) {
    if (query.getParams() == null || query.getParams().isEmpty()) {
      return Collections.emptyList();
    }
    List<QueryParamConfig> params = new ArrayList<>(query.getParams());
    params.sort(Comparator.comparingInt(QueryParamConfig::getIndex));
    int maxIndex = params.stream().mapToInt(QueryParamConfig::getIndex).max().orElse(0);
    List<Object> values = new ArrayList<>(Collections.nCopies(maxIndex, null));
    for (QueryParamConfig param : params) {
      if (param.getIndex() <= 0 || param.getSource() == null) {
        continue;
      }
      Object value = normalizeSqlParam(resolveParamValue(env, param.getSource()));
      values.set(param.getIndex() - 1, value);
    }
    return values;
  }

  private static CompletionStage<List<Object>> resolveQueryValuesAsync(Vertx vertx,
                                                                      QueryConfig query,
                                                                      DataFetchingEnvironment env) {
    if (query == null || query.getParams() == null || query.getParams().isEmpty()) {
      return CompletableFuture.completedFuture(Collections.emptyList());
    }
    List<QueryParamConfig> params = new ArrayList<>(query.getParams());
    params.sort(Comparator.comparingInt(QueryParamConfig::getIndex));
    int maxIndex = params.stream().mapToInt(QueryParamConfig::getIndex).max().orElse(0);
    List<CompletionStage<Object>> stages = new ArrayList<>(Collections.nCopies(maxIndex, CompletableFuture.completedFuture(null)));
    for (QueryParamConfig param : params) {
      if (param == null || param.getIndex() <= 0 || param.getSource() == null) {
        continue;
      }
      CompletionStage<Object> stage = resolveParamValueAsync(vertx, env, param.getSource())
        .thenApply(GraphQLServer::normalizeSqlParam);
      stages.set(param.getIndex() - 1, stage);
    }
    CompletableFuture<?>[] cfs = stages.stream().map(GraphQLServer::toCompletableFuture).toArray(CompletableFuture[]::new);
    return CompletableFuture.allOf(cfs).thenApply(ignored -> {
      List<Object> values = new ArrayList<>(Collections.nCopies(maxIndex, null));
      for (int i = 0; i < stages.size(); i++) {
        values.set(i, toCompletableFuture(stages.get(i)).join());
      }
      return values;
    });
  }

  private static Object normalizeSqlParam(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof io.vertx.core.json.JsonObject || value instanceof io.vertx.core.json.JsonArray) {
      try {
        return JSON_MAPPER.writeValueAsString(value);
      } catch (Exception e) {
        LOGGER.warn("SQL param json serialize failed err={}", e.getMessage());
        return value;
      }
    }
    if (value instanceof Map || value instanceof List || value.getClass().isArray()) {
      try {
        return JSON_MAPPER.writeValueAsString(value);
      } catch (Exception e) {
        LOGGER.warn("SQL param json serialize failed err={}", e.getMessage());
        return value;
      }
    }
    return value;
  }

  private static Map<String, Object> buildParamValueMap(QueryConfig query, DataFetchingEnvironment env) {
    if (query == null || query.getParams() == null || query.getParams().isEmpty()) {
      return Collections.emptyMap();
    }
    Map<String, Object> values = new HashMap<>();
    for (QueryParamConfig param : query.getParams()) {
      if (param == null || param.getSource() == null) {
        continue;
      }
      String name = param.getSource().getName();
      if (name == null || name.isBlank()) {
        continue;
      }
      values.put(name, resolveParamValue(env, param.getSource()));
    }
    return values;
  }

  private static Map<String, DuckDbConnectionConfig> indexDuckDbConnections(List<DuckDbConnectionConfig> connections) {
    Map<String, DuckDbConnectionConfig> indexed = new HashMap<>();
    if (connections == null || connections.isEmpty()) {
      return indexed;
    }
    for (DuckDbConnectionConfig connection : connections) {
      if (connection == null || connection.getName() == null || connection.getName().isBlank()) {
        continue;
      }
      indexed.put(connection.getName(), connection);
    }
    return indexed;
  }

  private static Map<String, JdbcConnectionConfig> indexJdbcConnections(List<JdbcConnectionConfig> connections) {
    Map<String, JdbcConnectionConfig> indexed = new HashMap<>();
    if (connections == null || connections.isEmpty()) {
      return indexed;
    }
    for (JdbcConnectionConfig connection : connections) {
      if (connection == null || connection.getName() == null || connection.getName().isBlank()) {
        continue;
      }
      indexed.put(connection.getName(), connection);
    }
    return indexed;
  }

  private static Map<String, MongoConnectionConfig> indexMongoConnections(List<MongoConnectionConfig> connections) {
    Map<String, MongoConnectionConfig> indexed = new HashMap<>();
    if (connections == null || connections.isEmpty()) {
      return indexed;
    }
    for (MongoConnectionConfig connection : connections) {
      if (connection == null || connection.getName() == null || connection.getName().isBlank()) {
        continue;
      }
      indexed.put(connection.getName(), connection);
    }
    return indexed;
  }

  private static Map<String, ElasticsearchConnectionConfig> indexElasticsearchConnections(
    List<ElasticsearchConnectionConfig> connections) {
    Map<String, ElasticsearchConnectionConfig> indexed = new HashMap<>();
    if (connections == null || connections.isEmpty()) {
      return indexed;
    }
    for (ElasticsearchConnectionConfig connection : connections) {
      if (connection == null || connection.getName() == null || connection.getName().isBlank()) {
        continue;
      }
      indexed.put(connection.getName(), connection);
    }
    return indexed;
  }

  private static Map<String, DynamoDbConnectionConfig> indexDynamoDbConnections(
    List<DynamoDbConnectionConfig> connections) {
    Map<String, DynamoDbConnectionConfig> indexed = new HashMap<>();
    if (connections == null || connections.isEmpty()) {
      return indexed;
    }
    for (DynamoDbConnectionConfig connection : connections) {
      if (connection == null || connection.getName() == null || connection.getName().isBlank()) {
        continue;
      }
      indexed.put(connection.getName(), connection);
    }
    return indexed;
  }

  private static Map<String, MongoClient> buildMongoClients(Map<String, MongoConnectionConfig> connections) {
    Map<String, MongoClient> clients = new HashMap<>();
    if (connections == null || connections.isEmpty()) {
      return clients;
    }
    for (MongoConnectionConfig connection : connections.values()) {
      if (connection == null || connection.getName() == null || connection.getName().isBlank()) {
        continue;
      }
      String uri = buildMongoConnectionString(connection);
      MongoClientSettings settings = MongoClientSettings.builder()
        .applyConnectionString(new ConnectionString(uri))
        .build();
      clients.put(connection.getName(), MongoClients.create(settings));
    }
    return clients;
  }

  private static String buildMongoConnectionString(MongoConnectionConfig connection) {
    String host = connection.getHost() == null ? "localhost" : connection.getHost();
    int port = connection.getPort() == null ? 27017 : connection.getPort();
    StringBuilder builder = new StringBuilder("mongodb://");
    String user = connection.getUser();
    String password = resolveMongoPassword(connection);
    if (user != null && !user.isBlank()) {
      builder.append(urlEncode(user));
      if (password != null && !password.isBlank()) {
        builder.append(':').append(urlEncode(password));
      }
      builder.append('@');
    }
    builder.append(host).append(':').append(port);
    String database = connection.getDatabase();
    if (database != null && !database.isBlank()) {
      builder.append('/').append(database);
    }
    List<String> params = new ArrayList<>();
    if (connection.getAuthSource() != null && !connection.getAuthSource().isBlank()) {
      params.add("authSource=" + urlEncode(connection.getAuthSource()));
    }
    if (Boolean.TRUE.equals(connection.getSsl())) {
      params.add("tls=true");
    }
    if (connection.getOptions() != null) {
      for (Map.Entry<String, String> entry : connection.getOptions().entrySet()) {
        if (entry.getKey() == null || entry.getKey().isBlank()) {
          continue;
        }
        String value = entry.getValue() == null ? "" : entry.getValue();
        params.add(urlEncode(entry.getKey()) + "=" + urlEncode(value));
      }
    }
    if (!params.isEmpty()) {
      builder.append('?').append(String.join("&", params));
    }
    return builder.toString();
  }

  private static String resolveMongoPassword(MongoConnectionConfig connection) {
    String password = connection.getPassword();
    if (password == null || password.isBlank()) {
      String envName = connection.getPasswordEnv();
      if (envName != null && !envName.isBlank()) {
        password = System.getenv(envName);
      }
    }
    return password;
  }

  private static Map<String, RestClient> buildElasticsearchClients(
    Map<String, ElasticsearchConnectionConfig> connections) {
    Map<String, RestClient> clients = new HashMap<>();
    if (connections == null || connections.isEmpty()) {
      return clients;
    }
    for (ElasticsearchConnectionConfig connection : connections.values()) {
      if (connection == null || connection.getName() == null || connection.getName().isBlank()) {
        continue;
      }
      String endpoint = connection.getEndpoint();
      if (endpoint == null || endpoint.isBlank()) {
        continue;
      }
      RestClientBuilder builder = RestClient.builder(HttpHost.create(endpoint));
      String apiKey = resolveElasticsearchApiKey(connection);
      if (apiKey != null && !apiKey.isBlank()) {
        builder.setDefaultHeaders(new org.apache.http.Header[] {
          new org.apache.http.message.BasicHeader("Authorization", "ApiKey " + apiKey)
        });
      } else if (connection.getUsername() != null && !connection.getUsername().isBlank()) {
        String password = resolveElasticsearchPassword(connection);
        org.apache.http.client.CredentialsProvider creds = new org.apache.http.impl.client.BasicCredentialsProvider();
        creds.setCredentials(org.apache.http.auth.AuthScope.ANY,
          new org.apache.http.auth.UsernamePasswordCredentials(connection.getUsername(), password));
        builder.setHttpClientConfigCallback(httpClientBuilder ->
          httpClientBuilder.setDefaultCredentialsProvider(creds));
      }
      clients.put(connection.getName(), builder.build());
    }
    return clients;
  }

  private static String resolveElasticsearchPassword(ElasticsearchConnectionConfig connection) {
    String password = connection.getPassword();
    if (password == null || password.isBlank()) {
      String envName = connection.getPasswordEnv();
      if (envName != null && !envName.isBlank()) {
        password = System.getenv(envName);
      }
    }
    return password;
  }

  private static String resolveElasticsearchApiKey(ElasticsearchConnectionConfig connection) {
    String apiKey = connection.getApiKey();
    if (apiKey == null || apiKey.isBlank()) {
      String envName = connection.getApiKeyEnv();
      if (envName != null && !envName.isBlank()) {
        apiKey = System.getenv(envName);
      }
    }
    return apiKey;
  }

  private static Map<String, DynamoDbClient> buildDynamoDbClients(
    Map<String, DynamoDbConnectionConfig> connections) {
    Map<String, DynamoDbClient> clients = new HashMap<>();
    if (connections == null || connections.isEmpty()) {
      return clients;
    }
    for (DynamoDbConnectionConfig connection : connections.values()) {
      if (connection == null || connection.getName() == null || connection.getName().isBlank()) {
        continue;
      }
      software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder builder = DynamoDbClient.builder();
      if (connection.getRegion() != null && !connection.getRegion().isBlank()) {
        builder.region(Region.of(connection.getRegion()));
      }
      if (connection.getEndpoint() != null && !connection.getEndpoint().isBlank()) {
        builder.endpointOverride(java.net.URI.create(connection.getEndpoint()));
      }
      String accessKey = resolveDynamoAccessKey(connection);
      String secretKey = resolveDynamoSecretKey(connection);
      if (accessKey != null && !accessKey.isBlank() && secretKey != null && !secretKey.isBlank()) {
        builder.credentialsProvider(StaticCredentialsProvider.create(
          AwsBasicCredentials.create(accessKey, secretKey)));
      }
      clients.put(connection.getName(), builder.build());
    }
    return clients;
  }

  private static String resolveDynamoAccessKey(DynamoDbConnectionConfig connection) {
    String accessKey = connection.getAccessKey();
    if (accessKey == null || accessKey.isBlank()) {
      String envName = connection.getAccessKeyEnv();
      if (envName != null && !envName.isBlank()) {
        accessKey = System.getenv(envName);
      }
    }
    return accessKey;
  }

  private static String resolveDynamoSecretKey(DynamoDbConnectionConfig connection) {
    String secretKey = connection.getSecretKey();
    if (secretKey == null || secretKey.isBlank()) {
      String envName = connection.getSecretKeyEnv();
      if (envName != null && !envName.isBlank()) {
        secretKey = System.getenv(envName);
      }
    }
    return secretKey;
  }

  private static String applyJsonTemplate(String template, Map<String, Object> values) {
    if (template == null || template.isBlank() || values == null || values.isEmpty()) {
      return template;
    }
    String rendered = template;
    for (Map.Entry<String, Object> entry : values.entrySet()) {
      if (entry.getKey() == null) {
        continue;
      }
      String placeholder = "{{" + entry.getKey() + "}}";
      if (!rendered.contains(placeholder)) {
        continue;
      }
      try {
        String replacement = JSON_MAPPER.writeValueAsString(entry.getValue());
        rendered = rendered.replace(placeholder, replacement);
      } catch (Exception e) {
        LOGGER.warn("Failed encoding template param {}", entry.getKey(), e);
      }
    }
    return rendered;
  }

  private static List<Bson> parseMongoPipeline(String pipelineJson) throws Exception {
    JsonNode node = JSON_MAPPER.readTree(pipelineJson);
    List<Bson> pipeline = new ArrayList<>();
    if (node.isArray()) {
      ArrayNode array = (ArrayNode) node;
      for (JsonNode stage : array) {
        pipeline.add(Document.parse(stage.toString()));
      }
    } else if (node.isObject()) {
      pipeline.add(Document.parse(node.toString()));
    } else {
      throw new IllegalStateException("Mongo pipeline must be a JSON array or object");
    }
    return pipeline;
  }

  private static Map<String, AttributeValue> buildDynamoAttributeValues(Map<String, Object> rawValues,
                                                                         Map<String, Object> params) {
    Map<String, AttributeValue> values = new HashMap<>();
    for (Map.Entry<String, Object> entry : rawValues.entrySet()) {
      if (entry.getKey() == null) {
        continue;
      }
      Object value = entry.getValue();
      if (value instanceof String) {
        String str = (String) value;
        if (str.startsWith("{{") && str.endsWith("}}")) {
          String key = str.substring(2, str.length() - 2);
          if (params != null && params.containsKey(key)) {
            value = params.get(key);
          }
        }
      }
      values.put(entry.getKey(), toAttributeValue(value));
    }
    return values;
  }

  private static Map<String, Object> convertDynamoItem(Map<String, AttributeValue> item) {
    Map<String, Object> converted = new HashMap<>();
    for (Map.Entry<String, AttributeValue> entry : item.entrySet()) {
      converted.put(entry.getKey(), attributeValueToObject(entry.getValue()));
    }
    return converted;
  }

  private static AttributeValue toAttributeValue(Object value) {
    if (value == null) {
      return AttributeValue.builder().nul(true).build();
    }
    if (value instanceof AttributeValue) {
      return (AttributeValue) value;
    }
    if (value instanceof String) {
      return AttributeValue.builder().s((String) value).build();
    }
    if (value instanceof Number) {
      return AttributeValue.builder().n(String.valueOf(value)).build();
    }
    if (value instanceof Boolean) {
      return AttributeValue.builder().bool((Boolean) value).build();
    }
    if (value instanceof Map) {
      @SuppressWarnings("unchecked")
      Map<String, Object> map = (Map<String, Object>) value;
      Map<String, AttributeValue> nested = new HashMap<>();
      for (Map.Entry<String, Object> entry : map.entrySet()) {
        nested.put(entry.getKey(), toAttributeValue(entry.getValue()));
      }
      return AttributeValue.builder().m(nested).build();
    }
    if (value instanceof List) {
      @SuppressWarnings("unchecked")
      List<Object> list = (List<Object>) value;
      List<AttributeValue> converted = new ArrayList<>();
      for (Object item : list) {
        converted.add(toAttributeValue(item));
      }
      return AttributeValue.builder().l(converted).build();
    }
    return AttributeValue.builder().s(String.valueOf(value)).build();
  }

  private static Object attributeValueToObject(AttributeValue value) {
    if (value == null) {
      return null;
    }
    if (Boolean.TRUE.equals(value.nul())) {
      return null;
    }
    if (value.s() != null) {
      return value.s();
    }
    if (value.n() != null) {
      String number = value.n();
      try {
        if (number.contains(".") || number.contains("e") || number.contains("E")) {
          return Double.parseDouble(number);
        }
        return Long.parseLong(number);
      } catch (NumberFormatException e) {
        return number;
      }
    }
    if (value.bool() != null) {
      return value.bool();
    }
    if (value.m() != null) {
      Map<String, Object> map = new HashMap<>();
      for (Map.Entry<String, AttributeValue> entry : value.m().entrySet()) {
        map.put(entry.getKey(), attributeValueToObject(entry.getValue()));
      }
      return map;
    }
    if (value.l() != null) {
      List<Object> list = new ArrayList<>();
      for (AttributeValue item : value.l()) {
        list.add(attributeValueToObject(item));
      }
      return list;
    }
    if (value.ss() != null && !value.ss().isEmpty()) {
      return new ArrayList<>(value.ss());
    }
    if (value.ns() != null && !value.ns().isEmpty()) {
      List<Object> list = new ArrayList<>();
      for (String num : value.ns()) {
        try {
          if (num.contains(".") || num.contains("e") || num.contains("E")) {
            list.add(Double.parseDouble(num));
          } else {
            list.add(Long.parseLong(num));
          }
        } catch (NumberFormatException e) {
          list.add(num);
        }
      }
      return list;
    }
    return null;
  }

  private static String urlEncode(String value) {
    try {
      return java.net.URLEncoder.encode(value, java.nio.charset.StandardCharsets.UTF_8.toString());
    } catch (Exception e) {
      return value;
    }
  }

  private static void validatePostgresMutations(List<PostgresMutationConfig> mutations,
                                                Map<String, PgPool> pools) {
    if (mutations == null || mutations.isEmpty()) {
      return;
    }
    if (pools.isEmpty()) {
      throw new IllegalStateException("Postgres mutations defined without a Postgres connection");
    }
  }

  private static void validatePostgresSubscriptions(List<PostgresSubscriptionConfig> subscriptions,
                                                    Map<String, ConnectionConfig> connectionIndex) {
    if (subscriptions == null || subscriptions.isEmpty()) {
      return;
    }
    if (connectionIndex.isEmpty()) {
      throw new IllegalStateException("Postgres subscriptions defined without a Postgres connection");
    }
    for (PostgresSubscriptionConfig subscription : subscriptions) {
      if (subscription == null) {
        continue;
      }
      String connectionRef = subscription.getConnectionRef();
      if (connectionRef == null || connectionRef.isBlank()) {
        throw new IllegalStateException("Postgres subscription missing connectionRef");
      }
      if (!connectionIndex.containsKey(connectionRef)) {
        throw new IllegalStateException("Connection " + connectionRef + " not configured");
      }
    }
  }

  private static Map<String, ConnectionConfig> indexConnections(List<ConnectionConfig> connections) {
    Map<String, ConnectionConfig> indexed = new HashMap<>();
    if (connections == null || connections.isEmpty()) {
      return indexed;
    }
    for (ConnectionConfig connection : connections) {
      if (connection == null || connection.getName() == null || connection.getName().isBlank()) {
        continue;
      }
      indexed.put(connection.getName(), connection);
    }
    return indexed;
  }

  private static final class PostgresReplicationManager {
    private final Vertx vertx;
    private final Map<String, ConnectionConfig> connectionIndex;
    private final Map<String, ReplicationStream> streams = new HashMap<>();

    private PostgresReplicationManager(Vertx vertx, Map<String, ConnectionConfig> connectionIndex) {
      this.vertx = vertx;
      this.connectionIndex = connectionIndex;
    }

    private synchronized SubscriptionHandle subscribe(PostgresSubscriptionConfig subscription,
                                                      DataFetchingEnvironment env,
                                                      Subscriber<? super Object> subscriber) {
      String connectionRef = subscription.getConnectionRef();
      ConnectionConfig connection = connectionIndex.get(connectionRef);
      if (connection == null) {
        subscriber.onError(new IllegalStateException("Connection " + connectionRef + " not configured"));
        return new SubscriptionHandle(null, null);
      }
      ReplicationStream stream = streams.computeIfAbsent(connectionRef,
        ref -> new ReplicationStream(vertx, connection, slotName(ref)));
      PostgresSubscriptionListener listener = new PostgresSubscriptionListener(subscription, env, subscriber, vertx);
      stream.addListener(listener);
      stream.ensureRunning();
      return new SubscriptionHandle(stream, listener);
    }

    private String slotName(String connectionRef) {
      StringBuilder builder = new StringBuilder("vertx_");
      for (int i = 0; i < connectionRef.length(); i++) {
        char ch = connectionRef.charAt(i);
        if ((ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9')) {
          builder.append(ch);
        } else if (ch >= 'A' && ch <= 'Z') {
          builder.append((char) (ch + 32));
        } else {
          builder.append('_');
        }
      }
      return builder.toString();
    }

    private final class SubscriptionHandle implements Subscription {
      private final ReplicationStream stream;
      private final PostgresSubscriptionListener listener;

      private SubscriptionHandle(ReplicationStream stream, PostgresSubscriptionListener listener) {
        this.stream = stream;
        this.listener = listener;
      }

      @Override
      public void request(long n) {
        // no backpressure support
      }

      @Override
      public void cancel() {
        if (stream == null || listener == null) {
          return;
        }
        listener.cancel();
        stream.removeListener(listener);
      }
    }

    private static final class ReplicationStream {
      private final Vertx vertx;
      private final ConnectionConfig connection;
      private final String slotName;
      private final List<PostgresSubscriptionListener> listeners = new ArrayList<>();
      private final AtomicBoolean running = new AtomicBoolean(false);
      private volatile java.sql.Connection replConnection;
      private volatile PGReplicationStream replicationStream;
      private Thread thread;

      private ReplicationStream(Vertx vertx, ConnectionConfig connection, String slotName) {
        this.vertx = vertx;
        this.connection = connection;
        this.slotName = slotName;
      }

      private synchronized void addListener(PostgresSubscriptionListener listener) {
        listeners.add(listener);
      }

      private synchronized void removeListener(PostgresSubscriptionListener listener) {
        listeners.remove(listener);
        if (listeners.isEmpty()) {
          LOGGER.info("Postgres replication slot {} has no listeners (stream remains running)", slotName);
        }
      }

      private synchronized void ensureRunning() {
        if (running.get()) {
          return;
        }
        LOGGER.info("Starting Postgres replication stream for slot {} (connection={})",
          slotName, connection.getName());
        running.set(true);
        thread = new Thread(this::runLoop, "pg-repl-" + slotName);
        thread.setDaemon(true);
        thread.start();
      }

      private synchronized void stop() {
        running.set(false);
        closeStream();
        if (thread != null) {
          thread.interrupt();
        }
      }

      private void runLoop() {
        try (java.sql.Connection replConn = openReplicationConnection(connection)) {
          this.replConnection = replConn;
          LOGGER.info("Opened Postgres replication connection for slot {} host={} port={} db={} user={}",
            slotName, connection.getHost(), connection.getPort(), connection.getDatabase(), connection.getUser());
          ensureReplicationSlot(connection, slotName);
          PGConnection pgConnection = replConn.unwrap(PGConnection.class);
          LogSequenceNumber lsn = fetchCurrentWAL(connection);
          LOGGER.info("Postgres replication slot {} starting at LSN {}", slotName, lsn);
          PGReplicationStream stream = pgConnection.getReplicationAPI()
            .replicationStream()
            .logical()
            .withSlotName(slotName)
            .withStartPosition(lsn)
            .withSlotOption("include-xids", false)
            .withSlotOption("include-timestamp", false)
            .withSlotOption("include-types", false)
            .withSlotOption("include-typmod", false)
            .withSlotOption("format-version", 2)
            .start();
          this.replicationStream = stream;

          while (running.get()) {
            ByteBuffer buffer = stream.read();
            if (buffer == null) {
              LOGGER.info("Postgres replication slot {} read null buffer", slotName);
              continue;
            }
            String message = decodeWalMessage(buffer);
            if (message != null) {
              int previewLimit = Math.min(message.length(), 500);
              LOGGER.info("Postgres replication slot {} received {} bytes preview={}",
                slotName, message.length(), message.substring(0, previewLimit));
            }
            if (message == null || message.isBlank()) {
              updateLsn(stream);
              continue;
            }
            List<PostgresChangeEvent> events = parseWal2Json(message);
            LOGGER.info("Postgres replication slot {} parsed {} events", slotName, events.size());
            for (PostgresChangeEvent event : events) {
              dispatch(event);
            }
            updateLsn(stream);
          }
        } catch (Exception e) {
          LOGGER.error("Postgres replication stream failed for slot {}", slotName, e);
        } finally {
          running.set(false);
          closeStream();
        }
      }

      private void closeStream() {
        PGReplicationStream stream = this.replicationStream;
        this.replicationStream = null;
        if (stream != null) {
          try {
            stream.close();
          } catch (SQLException ignored) {
          }
        }
        java.sql.Connection replConn = this.replConnection;
        this.replConnection = null;
        if (replConn != null) {
          try {
            replConn.close();
          } catch (SQLException ignored) {
          }
        }
      }

      private void dispatch(PostgresChangeEvent event) {
        List<PostgresSubscriptionListener> snapshot;
        synchronized (this) {
          if (listeners.isEmpty()) {
            LOGGER.info("Postgres replication slot {} skipping event with no listeners (table={} op={})",
              slotName, event.table, event.operation);
            return;
          }
          snapshot = new ArrayList<>(listeners);
        }
        LOGGER.info("Postgres replication slot {} dispatching event to {} listeners (table={} op={})",
          slotName, snapshot.size(), event.table, event.operation);
        for (PostgresSubscriptionListener listener : snapshot) {
          listener.handle(event);
        }
      }

      private void updateLsn(PGReplicationStream stream) throws SQLException {
        LogSequenceNumber lsn = stream.getLastReceiveLSN();
        if (lsn == null) {
          return;
        }
        stream.setAppliedLSN(lsn);
        stream.setFlushedLSN(lsn);
        stream.forceUpdateStatus();
      }
    }
  }

  private static final class PostgresSubscriptionListener {
    private final PostgresSubscriptionConfig subscription;
    private final DataFetchingEnvironment env;
    private final Subscriber<? super Object> subscriber;
    private final Vertx vertx;
    private final AtomicBoolean cancelled = new AtomicBoolean(false);

    private PostgresSubscriptionListener(PostgresSubscriptionConfig subscription,
                                         DataFetchingEnvironment env,
                                         Subscriber<? super Object> subscriber,
                                         Vertx vertx) {
      this.subscription = subscription;
      this.env = env;
      this.subscriber = subscriber;
      this.vertx = vertx;
    }

    private void handle(PostgresChangeEvent event) {
      if (cancelled.get()) {
        return;
      }
      if (!matchesTable(event.table, subscription.getTable())) {
        return;
      }
      if (!matchesOperation(event.operation, subscription.getOperations())) {
        return;
      }
      Map<String, Object> payload = new HashMap<>(event.data);
      payload.put("__table", event.table);
      payload.put("__op", event.operation.name());
      vertx.runOnContext(ignored -> {
        if (!matchesPostgresFilters(payload, subscription.getFilters(), env)) {
          return;
        }
        try {
          LOGGER.info("Postgres subscription {} delivering table={} op={}",
            subscription.getFieldName(), event.table, event.operation);
          subscriber.onNext(payload);
        } catch (Exception e) {
          LOGGER.error("Postgres subscription {} failed delivering payload", subscription.getFieldName(), e);
          subscriber.onError(e);
        }
      });
    }

    private void cancel() {
      cancelled.set(true);
    }
  }

  private static boolean matchesTable(String eventTable, String subscriptionTable) {
    if (subscriptionTable == null || subscriptionTable.isBlank()) {
      return true;
    }
    if (eventTable == null) {
      return false;
    }
    String normalizedSub = subscriptionTable.trim().toLowerCase(Locale.ROOT);
    String normalizedEvent = eventTable.trim().toLowerCase(Locale.ROOT);
    if (normalizedSub.contains(".")) {
      return normalizedEvent.equals(normalizedSub);
    }
    int dot = normalizedEvent.indexOf('.');
    if (dot >= 0) {
      normalizedEvent = normalizedEvent.substring(dot + 1);
    }
    return normalizedEvent.equals(normalizedSub);
  }

  private static boolean matchesOperation(PostgresChangeEvent.Operation op,
                                          List<PostgresSubscriptionConfig.Operation> operations) {
    if (operations == null || operations.isEmpty()) {
      return true;
    }
    for (PostgresSubscriptionConfig.Operation operation : operations) {
      if (operation != null && operation.name().equals(op.name())) {
        return true;
      }
    }
    return false;
  }

  private static List<PostgresChangeEvent> parseWal2Json(String message) {
    if (message == null || message.isBlank()) {
      return Collections.emptyList();
    }
    JsonObject payload;
    try {
      payload = new JsonObject(message);
    } catch (RuntimeException e) {
      LOGGER.warn("Failed to parse wal2json payload", e);
      return Collections.emptyList();
    }
    if (payload.containsKey("action")) {
      return parseLegacyWal2Json(payload);
    }
    JsonArray changes = payload.getJsonArray("change");
    if (changes == null || changes.isEmpty()) {
      return Collections.emptyList();
    }
    LOGGER.info("wal2json change count={}", changes.size());
    List<PostgresChangeEvent> events = new ArrayList<>();
    for (int i = 0; i < changes.size(); i++) {
      Object raw = changes.getValue(i);
      if (!(raw instanceof JsonObject)) {
        continue;
      }
      JsonObject change = (JsonObject) raw;
      String kind = change.getString("kind");
      String schema = change.getString("schema");
      String table = change.getString("table");
      PostgresChangeEvent.Operation op = mapWal2JsonOperation(kind);
      if (op == null) {
        continue;
      }
      String fullTable = schema == null ? table : schema + "." + table;
      Map<String, Object> data = new HashMap<>();
      JsonArray columnNames = change.getJsonArray("columnnames");
      JsonArray columnValues = change.getJsonArray("columnvalues");
      if (columnNames != null && columnValues != null && columnNames.size() == columnValues.size()) {
        for (int idx = 0; idx < columnNames.size(); idx++) {
          String name = columnNames.getString(idx);
          Object value = columnValues.getValue(idx);
          data.put(name, normalizeWal2JsonValue(value));
        }
      } else if (change.containsKey("oldkeys")) {
        JsonObject oldKeys = change.getJsonObject("oldkeys");
        JsonArray oldNames = oldKeys.getJsonArray("keynames");
        JsonArray oldValues = oldKeys.getJsonArray("keyvalues");
        if (oldNames != null && oldValues != null && oldNames.size() == oldValues.size()) {
          for (int idx = 0; idx < oldNames.size(); idx++) {
            String name = oldNames.getString(idx);
            Object value = oldValues.getValue(idx);
            data.put(name, normalizeWal2JsonValue(value));
          }
        }
      }
      events.add(new PostgresChangeEvent(fullTable, op, data));
    }
    return events;
  }

  private static List<PostgresChangeEvent> parseLegacyWal2Json(JsonObject payload) {
    String action = payload.getString("action");
    PostgresChangeEvent.Operation op = mapLegacyWal2JsonOperation(action);
    if (op == null) {
      return Collections.emptyList();
    }
    String schema = payload.getString("schema");
    String table = payload.getString("table");
    String fullTable = schema == null ? table : schema + "." + table;
    Map<String, Object> data = new HashMap<>();
    JsonArray columns = payload.getJsonArray("columns");
    if (columns != null) {
      for (int i = 0; i < columns.size(); i++) {
        Object raw = columns.getValue(i);
        if (!(raw instanceof JsonObject)) {
          continue;
        }
        JsonObject column = (JsonObject) raw;
        String name = column.getString("name");
        Object value = column.getValue("value");
        if (name != null) {
          data.put(name, normalizeWal2JsonValue(value));
        }
      }
    }
    return Collections.singletonList(new PostgresChangeEvent(fullTable, op, data));
  }

  private static PostgresChangeEvent.Operation mapWal2JsonOperation(String kind) {
    if (kind == null) {
      return null;
    }
    switch (kind.toLowerCase(Locale.ROOT)) {
      case "insert":
        return PostgresChangeEvent.Operation.INSERT;
      case "update":
        return PostgresChangeEvent.Operation.UPDATE;
      case "delete":
        return PostgresChangeEvent.Operation.DELETE;
      default:
        return null;
    }
  }

  private static PostgresChangeEvent.Operation mapLegacyWal2JsonOperation(String action) {
    if (action == null) {
      return null;
    }
    switch (action.toUpperCase(Locale.ROOT)) {
      case "I":
        return PostgresChangeEvent.Operation.INSERT;
      case "U":
        return PostgresChangeEvent.Operation.UPDATE;
      case "D":
        return PostgresChangeEvent.Operation.DELETE;
      default:
        return null;
    }
  }

  private static Object normalizeWal2JsonValue(Object value) {
    if (value instanceof String) {
      String trimmed = ((String) value).trim();
      if ((trimmed.startsWith("{") && trimmed.endsWith("}"))
        || (trimmed.startsWith("[") && trimmed.endsWith("]"))) {
        try {
          if (trimmed.startsWith("{")) {
            return new JsonObject(trimmed).getMap();
          }
          return new JsonArray(trimmed).getList();
        } catch (RuntimeException ignored) {
          return value;
        }
      }
    }
    return value;
  }

  private static String decodeWalMessage(ByteBuffer buffer) {
    if (buffer == null) {
      return null;
    }
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  private static java.sql.Connection openReplicationConnection(ConnectionConfig connection) throws SQLException {
    String url = postgresJdbcUrl(connection);
    java.util.Properties props = new java.util.Properties();
    String password = resolveConnectionPassword(connection);
    PGProperty.USER.set(props, connection.getUser());
    if (password != null && !password.isBlank()) {
      PGProperty.PASSWORD.set(props, password);
    }
    PGProperty.REPLICATION.set(props, "database");
    PGProperty.PREFER_QUERY_MODE.set(props, "simple");
    PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "9.4");
    if (Boolean.TRUE.equals(connection.getSsl())) {
      props.setProperty("ssl", "true");
    }
    return DriverManager.getConnection(url, props);
  }

  private static void ensureReplicationSlot(ConnectionConfig connection, String slotName) throws SQLException {
    try (java.sql.Connection adminConn = openStandardConnection(connection);
         Statement statement = adminConn.createStatement()) {
      try {
        LOGGER.info("Ensuring Postgres replication slot {} exists", slotName);
        statement.execute("SELECT pg_create_logical_replication_slot('" + slotName + "', 'wal2json')");
        LOGGER.info("Created Postgres replication slot {}", slotName);
      } catch (SQLException createError) {
        String message = createError.getMessage();
        if (message != null && message.contains("already exists")) {
          LOGGER.info("Postgres replication slot {} already exists", slotName);
          return;
        }
        LOGGER.error("Failed creating Postgres replication slot {}: {}", slotName, message);
        if (message == null || !message.contains("already exists")) {
          throw createError;
        }
      }
    }
  }

  private static java.sql.Connection openStandardConnection(ConnectionConfig connection) throws SQLException {
    String url = postgresJdbcUrl(connection);
    java.util.Properties props = new java.util.Properties();
    String password = resolveConnectionPassword(connection);
    PGProperty.USER.set(props, connection.getUser());
    if (password != null && !password.isBlank()) {
      PGProperty.PASSWORD.set(props, password);
    }
    if (Boolean.TRUE.equals(connection.getSsl())) {
      props.setProperty("ssl", "true");
    }
    return DriverManager.getConnection(url, props);
  }

  private static LogSequenceNumber fetchCurrentWAL(ConnectionConfig connection) throws SQLException {
    try (java.sql.Connection conn = openStandardConnection(connection);
         Statement statement = conn.createStatement();
         ResultSet rs = statement.executeQuery("SELECT pg_current_wal_lsn()")) {
      if (!rs.next()) {
        throw new SQLException("Failed to read current WAL LSN");
      }
      String lsn = rs.getString(1);
      return LogSequenceNumber.valueOf(lsn);
    }
  }

  private static String postgresJdbcUrl(ConnectionConfig connection) {
    String host = connection.getHost();
    int port = connection.getPort() == null ? 5432 : connection.getPort();
    String database = connection.getDatabase();
    return "jdbc:postgresql://" + host + ":" + port + "/" + database;
  }

  private static String resolveConnectionPassword(ConnectionConfig connection) {
    String password = connection.getPassword();
    if (password == null || password.isBlank()) {
      String envName = connection.getPasswordEnv();
      if (envName != null && !envName.isBlank()) {
        password = System.getenv(envName);
      }
    }
    return password;
  }

  private static final class PostgresChangeEvent {
    private enum Operation {
      INSERT,
      UPDATE,
      DELETE
    }

    private final String table;
    private final Operation operation;
    private final Map<String, Object> data;

    private PostgresChangeEvent(String table, Operation operation, Map<String, Object> data) {
      this.table = table;
      this.operation = operation;
      this.data = data;
    }
  }

  private static final class MutationPayload {
    private final String argName;
    private final Object payload;

    private MutationPayload(String argName, Object payload) {
      this.argName = argName;
      this.payload = payload;
    }
  }

  private static void validateQueryConnections(List<QueryConfig> queries,
                                               Map<String, PgPool> pools,
                                               Map<String, DuckDbConnectionConfig> duckdbConnections,
                                               Map<String, JdbcConnectionConfig> jdbcConnections,
                                               Map<String, MongoConnectionConfig> mongoConnections,
                                               Map<String, ElasticsearchConnectionConfig> elasticsearchConnections,
                                               Map<String, DynamoDbConnectionConfig> dynamodbConnections) {
    if (queries == null || queries.isEmpty()) {
      return;
    }
    boolean hasPostgres = false;
    boolean hasDuckdb = false;
    boolean hasJdbc = false;
    boolean hasMongo = false;
    boolean hasElasticsearch = false;
    boolean hasDynamo = false;
    for (QueryConfig query : queries) {
      if (query == null) {
        continue;
      }
      QueryConfig.Engine engine = query.getEngine();
      if (engine == QueryConfig.Engine.DUCKDB) {
        hasDuckdb = true;
      } else if (engine == QueryConfig.Engine.JDBC) {
        hasJdbc = true;
      } else if (engine == QueryConfig.Engine.MONGODB) {
        hasMongo = true;
      } else if (engine == QueryConfig.Engine.ELASTICSEARCH) {
        hasElasticsearch = true;
      } else if (engine == QueryConfig.Engine.DYNAMODB) {
        hasDynamo = true;
      } else {
        hasPostgres = true;
      }
    }
    if (hasPostgres && pools.isEmpty()) {
      throw new IllegalStateException("Postgres queries defined without a Postgres connection");
    }
    if (hasDuckdb && duckdbConnections.isEmpty()) {
      throw new IllegalStateException("DuckDB queries defined without a DuckDB connection");
    }
    if (hasJdbc && jdbcConnections.isEmpty()) {
      throw new IllegalStateException("JDBC queries defined without a JDBC connection");
    }
    if (hasMongo && mongoConnections.isEmpty()) {
      throw new IllegalStateException("MongoDB queries defined without a MongoDB connection");
    }
    if (hasElasticsearch && elasticsearchConnections.isEmpty()) {
      throw new IllegalStateException("Elasticsearch queries defined without an Elasticsearch connection");
    }
    if (hasDynamo && dynamodbConnections.isEmpty()) {
      throw new IllegalStateException("DynamoDB queries defined without a DynamoDB connection");
    }
  }

  private static void configurePagination(List<QueryConfig> queries, AuthConfig authConfig) {
    PAGINATION_SIGNERS.clear();
    if (queries == null || queries.isEmpty()) {
      return;
    }
    for (QueryConfig query : queries) {
      if (query == null || query.getPagination() == null) {
        continue;
      }
      PaginationConfig pagination = query.getPagination();
      if (pagination.getCursorFields() == null || pagination.getCursorFields().isEmpty()) {
        throw new IllegalStateException("Pagination configured without cursorFields for query " + query.getName());
      }
      CursorSigner signer = buildCursorSigner(query, pagination, authConfig);
      if (signer == null) {
        throw new IllegalStateException("Pagination configured without signing secret for query " + query.getName());
      }
      PAGINATION_SIGNERS.put(query.getName(), signer);
    }
  }

  private static CursorSigner buildCursorSigner(QueryConfig query,
                                                PaginationConfig pagination,
                                                AuthConfig authConfig) {
    CursorSigningConfig signing = pagination.getSigning();
    if (signing == null) {
      CursorSigningConfig fallback = new CursorSigningConfig();
      if (authConfig != null && authConfig.getJwt() != null
        && resolveAuthJwtSecret(authConfig.getJwt()) != null) {
        fallback.setMethod(CursorSigningConfig.Method.JWT);
        fallback.setUseAuthJwtSecret(true);
      } else {
        return null;
      }
      signing = fallback;
    }

    CursorSigningConfig.Method method = signing.getMethod();
    String secret = resolveSigningSecret(signing, authConfig);
    if (secret == null || secret.isBlank()) {
      LOGGER.warn("Pagination signing secret missing for query={}", query.getName());
      return null;
    }
    if (method == CursorSigningConfig.Method.JWT) {
      return new JwtCursorSigner(secret);
    }
    return new HmacCursorSigner(secret);
  }

  private static String resolveSigningSecret(CursorSigningConfig signing, AuthConfig authConfig) {
    if (signing == null) {
      return null;
    }
    if (signing.getSharedSecret() != null && !signing.getSharedSecret().isBlank()) {
      return signing.getSharedSecret();
    }
    if (signing.getSharedSecretEnv() != null && !signing.getSharedSecretEnv().isBlank()) {
      String value = System.getenv(signing.getSharedSecretEnv());
      if (value != null && !value.isBlank()) {
        return value;
      }
    }
    if (Boolean.TRUE.equals(signing.getUseAuthJwtSecret()) && authConfig != null && authConfig.getJwt() != null) {
      return resolveAuthJwtSecret(authConfig.getJwt());
    }
    return null;
  }

  private static String resolveAuthJwtSecret(JwtAuthConfig jwtConfig) {
    if (jwtConfig == null) {
      return null;
    }
    if (jwtConfig.getSharedSecret() != null && !jwtConfig.getSharedSecret().isBlank()) {
      return jwtConfig.getSharedSecret();
    }
    if (jwtConfig.getSharedSecretEnv() != null && !jwtConfig.getSharedSecretEnv().isBlank()) {
      String value = System.getenv(jwtConfig.getSharedSecretEnv());
      if (value != null && !value.isBlank()) {
        return value;
      }
    }
    return null;
  }

  private static Map<String, PgPool> buildPgPools(Vertx vertx, List<ConnectionConfig> connections) {
    Map<String, PgPool> pools = new HashMap<>();
    if (connections == null || connections.isEmpty()) {
      return pools;
    }
    for (ConnectionConfig connection : connections) {
      if (connection == null || connection.getName() == null || connection.getName().isBlank()) {
        continue;
      }
      PoolOptions poolOptions = new PoolOptions().setMaxSize(5);
      ConnectionConfig.PoolConfig pool = connection.getPool();
      if (pool != null) {
        if (pool.getMaxSize() != null && pool.getMaxSize() > 0) {
          poolOptions.setMaxSize(pool.getMaxSize());
        }
        if (pool.getMaxWaitQueueSize() != null && pool.getMaxWaitQueueSize() > 0) {
          poolOptions.setMaxWaitQueueSize(pool.getMaxWaitQueueSize());
        }
        if (pool.getIdleTimeoutSeconds() != null && pool.getIdleTimeoutSeconds() > 0) {
          poolOptions.setIdleTimeout(pool.getIdleTimeoutSeconds());
        }
        if (pool.getConnectionTimeoutSeconds() != null && pool.getConnectionTimeoutSeconds() > 0) {
          poolOptions.setConnectionTimeout(pool.getConnectionTimeoutSeconds() * 1000);
        }
      }
      PgPool poolClient = PgPool.pool(vertx, buildConnectOptions(connection), poolOptions);
      pools.put(connection.getName(), poolClient);
    }
    return pools;
  }

  private static void executePgPreparedWithStatementTimeout(PgPool pool,
                                                            String sql,
                                                            Tuple tuple,
                                                            int timeoutMs,
                                                            io.vertx.core.Handler<io.vertx.core.AsyncResult<io.vertx.sqlclient.RowSet<io.vertx.sqlclient.Row>>> handler) {
    if (pool == null) {
      handler.handle(io.vertx.core.Future.failedFuture(new IllegalStateException("No Postgres pool available")));
      return;
    }
    if (sql == null || sql.isBlank()) {
      handler.handle(io.vertx.core.Future.failedFuture(new IllegalStateException("SQL is required")));
      return;
    }
    if (timeoutMs <= 0) {
      pool.preparedQuery(sql).execute(tuple, handler);
      return;
    }

    pool.getConnection(connAr -> {
      if (connAr.failed()) {
        handler.handle(io.vertx.core.Future.failedFuture(connAr.cause()));
        return;
      }
      io.vertx.sqlclient.SqlConnection conn = connAr.result();
      conn.begin(txAr -> {
        if (txAr.failed()) {
          conn.close();
          handler.handle(io.vertx.core.Future.failedFuture(txAr.cause()));
          return;
        }
        io.vertx.sqlclient.Transaction tx = txAr.result();
        conn.query("SET LOCAL statement_timeout = " + timeoutMs).execute(setAr -> {
          if (setAr.failed()) {
            tx.rollback(ignored -> conn.close());
            handler.handle(io.vertx.core.Future.failedFuture(setAr.cause()));
            return;
          }
          conn.preparedQuery(sql).execute(tuple, queryAr -> {
            if (queryAr.failed()) {
              tx.rollback(ignored -> conn.close());
              handler.handle(io.vertx.core.Future.failedFuture(queryAr.cause()));
              return;
            }
            tx.commit(commitAr -> {
              conn.close();
              if (commitAr.failed()) {
                handler.handle(io.vertx.core.Future.failedFuture(commitAr.cause()));
              } else {
                handler.handle(io.vertx.core.Future.succeededFuture(queryAr.result()));
              }
            });
          });
        });
      });
    });
  }

  private static <T> CompletableFuture<T> toCompletableFuture(CompletionStage<T> stage) {
    if (stage == null) {
      return CompletableFuture.completedFuture(null);
    }
    if (stage instanceof CompletableFuture) {
      return (CompletableFuture<T>) stage;
    }
    CompletableFuture<T> cf = new CompletableFuture<>();
    stage.whenComplete((value, err) -> {
      if (err != null) {
        cf.completeExceptionally(err);
      } else {
        cf.complete(value);
      }
    });
    return cf;
  }

  private static <T> CompletableFuture<T> failedFuture(Throwable err) {
    CompletableFuture<T> cf = new CompletableFuture<>();
    cf.completeExceptionally(err);
    return cf;
  }

  private static CompletionStage<Object> resolveParamValueAsync(Vertx vertx,
                                                                graphql.schema.DataFetchingEnvironment env,
                                                                ParamSourceConfig source) {
    if (source == null || source.getKind() == null) {
      return CompletableFuture.completedFuture(null);
    }
    if (source.getKind() != ParamSourceConfig.Kind.PYTHON) {
      return CompletableFuture.completedFuture(resolveParamValue(env, source));
    }
    if (vertx == null) {
      return failedFuture(new IllegalStateException("Python param resolution requires Vertx"));
    }
    if (PYTHON_FUNCTIONS == null || !PYTHON_FUNCTIONS.isEnabled()) {
      return failedFuture(new IllegalStateException("Python functions are not configured"));
    }

    ParamSourceConfig.PythonInvocation invocation = source.getPython();
    String functionRef = invocation != null && invocation.getFunctionRef() != null && !invocation.getFunctionRef().isBlank()
      ? invocation.getFunctionRef()
      : source.getName();
    if (functionRef == null || functionRef.isBlank()) {
      return failedFuture(new IllegalStateException("PYTHON param missing functionRef (use source.name or source.python.functionRef)"));
    }

    List<ParamSourceConfig> argSources = invocation == null ? null : invocation.getArgs();
    if (argSources == null || argSources.isEmpty()) {
      return PYTHON_FUNCTIONS.invokeAsync(vertx, functionRef, List.of(), invocation == null ? null : invocation.getTimeoutMs());
    }

    List<CompletionStage<Object>> stages = new ArrayList<>();
    for (ParamSourceConfig arg : argSources) {
      stages.add(resolveParamValueAsync(vertx, env, arg));
    }
    CompletableFuture<?>[] cfs = stages.stream().map(GraphQLServer::toCompletableFuture).toArray(CompletableFuture[]::new);
    return CompletableFuture.allOf(cfs).thenCompose(ignored -> {
      List<Object> args = new ArrayList<>();
      for (CompletionStage<Object> st : stages) {
        args.add(toCompletableFuture(st).join());
      }
      Integer timeoutMs = invocation == null ? null : invocation.getTimeoutMs();
      return PYTHON_FUNCTIONS.invokeAsync(vertx, functionRef, args, timeoutMs);
    });
  }

  private static Object resolveParamValue(graphql.schema.DataFetchingEnvironment env, ParamSourceConfig source) {
    Object value = null;
    if (source.getKind() == ParamSourceConfig.Kind.ARG) {
      value = resolveArgParamValue(env, source.getName());
      PaginationContext paginationContext = paginationContext(env);
      if (paginationContext != null
        && paginationContext.firstArg != null
        && paginationContext.firstArg.equals(source.getName())) {
        value = paginationContext.sqlLimit;
      }
    } else if (source.getKind() == ParamSourceConfig.Kind.RAW) {
      value = env.getArguments();
    } else if (source.getKind() == ParamSourceConfig.Kind.HEADER) {
      MultiMap headers = headersFromContext(env.getGraphQlContext());
      if (headers != null) {
        value = headers.get(source.getName());
      }
    } else if (source.getKind() == ParamSourceConfig.Kind.ENV) {
      value = System.getenv(source.getName());
    } else if (source.getKind() == ParamSourceConfig.Kind.PARENT) {
      Object parent = env.getSource();
      if (parent instanceof Map) {
        value = ((Map<?, ?>) parent).get(source.getName());
      }
    } else if (source.getKind() == ParamSourceConfig.Kind.JWT) {
      value = resolveJwtParam(env, source.getName());
    } else if (source.getKind() == ParamSourceConfig.Kind.CURSOR) {
      value = resolveCursorParam(env, source.getName());
    } else if (source.getKind() == ParamSourceConfig.Kind.PYTHON) {
      throw new IllegalStateException("PYTHON ParamSource must be resolved asynchronously");
    }
    return value;
  }

  private static Object resolveArgParamValue(graphql.schema.DataFetchingEnvironment env, String name) {
    if (env == null || name == null || name.isBlank()) {
      return null;
    }
    String key = name.trim();
    Object direct = env.getArgument(key);
    if (direct != null) {
      return direct;
    }
    Map<String, Object> args = env.getArguments();
    if (args == null || args.isEmpty()) {
      return null;
    }
    String jsonPath = null;
    if (key.startsWith("$")) {
      jsonPath = key;
    } else if (key.contains(".") || key.contains("[")) {
      jsonPath = "$." + key;
    }
    if (jsonPath != null) {
      try {
        return JsonPath.read(args, jsonPath);
      } catch (Exception e) {
        LOGGER.warn("ARG jsonpath resolve failed path={} err={}", jsonPath, e.getMessage());
      }
    }
    return null;
  }

  private static PaginationContext paginationContext(graphql.schema.DataFetchingEnvironment env) {
    if (env == null) {
      return null;
    }
    GraphQLContext ctx = env.getGraphQlContext();
    if (ctx == null) {
      return null;
    }
    Object value = ctx.get(PAGINATION_CONTEXT_KEY);
    if (value instanceof PaginationContext) {
      return (PaginationContext) value;
    }
    return null;
  }

  private static Object resolveCursorParam(graphql.schema.DataFetchingEnvironment env, String name) {
    if (name == null || name.isBlank()) {
      return null;
    }
    PaginationContext context = paginationContext(env);
    if (context == null || context.afterPayload == null || context.afterPayload.isEmpty()) {
      return null;
    }
    String key = name.trim();
    if (key.startsWith("$")) {
      try {
        return JsonPath.read(context.afterPayload, key);
      } catch (Exception e) {
        LOGGER.warn("Cursor jsonpath resolve failed path={} err={}", key, e.getMessage());
        return null;
      }
    }
    Object direct = context.afterPayload.get(key);
    if (direct != null) {
      Object converted = convertCursorValue(direct);
      return converted;
    }
    return resolveNestedClaim(context.afterPayload, key);
  }

  private static Object convertCursorValue(Object value) {
    if (value instanceof String) {
      String text = (String) value;
      try {
        return java.time.OffsetDateTime.parse(text);
      } catch (Exception ignored) {
        return value;
      }
    }
    return value;
  }

  private static Object resolveJwtParam(graphql.schema.DataFetchingEnvironment env, String name) {
    if (name == null || name.isBlank()) {
      return null;
    }
    AuthContext authContext = getAuthContext(env);
    if (authContext == null) {
      return null;
    }
    String key = name.trim();
    if (key.startsWith("$")) {
      Map<String, Object> root = new HashMap<>();
      Map<String, Object> claims = authContext.getClaims();
      if (claims != null && !claims.isEmpty()) {
        root.putAll(claims);
      }
      root.put("sub", authContext.getSubject());
      root.put("subject", authContext.getSubject());
      root.put("roles", new ArrayList<>(authContext.getRoles()));
      root.put("scopes", new ArrayList<>(authContext.getScopes()));
      try {
        return JsonPath.read(root, key);
      } catch (Exception e) {
        LOGGER.warn("JWT jsonpath resolve failed path={} err={}", key, e.getMessage());
        return null;
      }
    }
    if ("sub".equalsIgnoreCase(key) || "subject".equalsIgnoreCase(key)) {
      return authContext.getSubject();
    }
    if ("roles".equalsIgnoreCase(key)) {
      return new ArrayList<>(authContext.getRoles());
    }
    if ("scopes".equalsIgnoreCase(key) || "scope".equalsIgnoreCase(key)) {
      return new ArrayList<>(authContext.getScopes());
    }
    Map<String, Object> claims = authContext.getClaims();
    if (claims == null || claims.isEmpty()) {
      return null;
    }
    Object direct = claims.get(key);
    if (direct != null) {
      return direct;
    }
    return resolveNestedClaim(claims, key);
  }

  private static Object resolveNestedClaim(Map<String, Object> claims, String path) {
    if (claims == null || path == null || path.isBlank() || !path.contains(".")) {
      return null;
    }
    String[] parts = path.split("\\.");
    Object current = claims;
    for (String part : parts) {
      if (!(current instanceof Map)) {
        return null;
      }
      current = ((Map<?, ?>) current).get(part);
      if (current == null) {
        return null;
      }
    }
    return current;
  }

  private static final class PaginationContext {
    private final PaginationConfig pagination;
    private final CursorSigner signer;
    private final String firstArg;
    private final String afterArg;
    private final int pageSize;
    private final int sqlLimit;
    private final boolean fetchExtra;
    private final Map<String, Object> afterPayload;

    private PaginationContext(PaginationConfig pagination,
                              CursorSigner signer,
                              String firstArg,
                              String afterArg,
                              int pageSize,
                              int sqlLimit,
                              Map<String, Object> afterPayload) {
      this.pagination = pagination;
      this.signer = signer;
      this.firstArg = firstArg;
      this.afterArg = afterArg;
      this.pageSize = pageSize;
      this.sqlLimit = sqlLimit;
      this.fetchExtra = !Boolean.FALSE.equals(pagination.getFetchExtra());
      this.afterPayload = afterPayload;
    }
  }

  private interface CursorSigner {
    String encode(Map<String, Object> payload);

    Map<String, Object> decode(String token);
  }

  private static final class HmacCursorSigner implements CursorSigner {
    private final byte[] secret;

    private HmacCursorSigner(String secret) {
      this.secret = secret.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public String encode(Map<String, Object> payload) {
      try {
        byte[] json = CURSOR_MAPPER.writeValueAsBytes(payload);
        String payloadB64 = Base64.getUrlEncoder().withoutPadding().encodeToString(json);
        byte[] sig = hmacSha256(payloadB64.getBytes(StandardCharsets.UTF_8));
        String sigB64 = Base64.getUrlEncoder().withoutPadding().encodeToString(sig);
        return payloadB64 + "." + sigB64;
      } catch (Exception e) {
        throw new IllegalStateException("Failed to encode cursor", e);
      }
    }

    @Override
    public Map<String, Object> decode(String token) {
      if (token == null || token.isBlank()) {
        return null;
      }
      String[] parts = token.split("\\.");
      if (parts.length != 2) {
        return null;
      }
      String payloadB64 = parts[0];
      String sigB64 = parts[1];
      byte[] expected = hmacSha256(payloadB64.getBytes(StandardCharsets.UTF_8));
      byte[] actual;
      try {
        actual = Base64.getUrlDecoder().decode(sigB64);
      } catch (IllegalArgumentException e) {
        return null;
      }
      if (!MessageDigest.isEqual(expected, actual)) {
        return null;
      }
      try {
        byte[] payloadJson = Base64.getUrlDecoder().decode(payloadB64);
        return CURSOR_MAPPER.readValue(payloadJson, new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {});
      } catch (Exception e) {
        return null;
      }
    }

    private byte[] hmacSha256(byte[] data) {
      try {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(secret, "HmacSHA256"));
        return mac.doFinal(data);
      } catch (Exception e) {
        throw new IllegalStateException("HMAC error", e);
      }
    }
  }

  private static final class JwtCursorSigner implements CursorSigner {
    private final byte[] secret;

    private JwtCursorSigner(String secret) {
      this.secret = secret.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public String encode(Map<String, Object> payload) {
      try {
        JWTClaimsSet.Builder builder = new JWTClaimsSet.Builder();
        if (payload != null) {
          for (Map.Entry<String, Object> entry : payload.entrySet()) {
            builder.claim(entry.getKey(), entry.getValue());
          }
        }
        builder.issueTime(new java.util.Date());
        SignedJWT jwt = new SignedJWT(new JWSHeader(com.nimbusds.jose.JWSAlgorithm.HS256), builder.build());
        JWSSigner signer = new MACSigner(secret);
        jwt.sign(signer);
        return jwt.serialize();
      } catch (Exception e) {
        throw new IllegalStateException("Failed to encode cursor", e);
      }
    }

    @Override
    public Map<String, Object> decode(String token) {
      if (token == null || token.isBlank()) {
        return null;
      }
      try {
        SignedJWT jwt = SignedJWT.parse(token);
        JWSVerifier verifier = new MACVerifier(secret);
        if (!jwt.verify(verifier)) {
          return null;
        }
        return jwt.getJWTClaimsSet().getClaims();
      } catch (Exception e) {
        return null;
      }
    }
  }

  private static MultiMap headersFromContext(GraphQLContext context) {
    if (context == null) {
      return null;
    }
    Object headers = context.get("headers");
    if (headers instanceof MultiMap) {
      return (MultiMap) headers;
    }
    return null;
  }

}
