package dev.henneberger.operator;

import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Kind;
import io.fabric8.kubernetes.model.annotation.Version;
import io.fabric8.kubernetes.model.annotation.Plural;

@Group("dev.henneberger")
@Version("v1alpha1")
@Kind("FlinkCatalog")
@Plural("flinkcatalogs")
public class FlinkCatalog extends CustomResource<FlinkCatalogSpec, FlinkCatalogStatus>
    implements Namespaced {}
