package dev.henneberger.operator.crd;

import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Kind;
import io.fabric8.kubernetes.model.annotation.Version;

@Group("vertx.henneberger.dev")
@Version("v1alpha1")
@Kind("Query")
public class Query extends CustomResource<QuerySpec, QueryStatus> implements Namespaced {
}
