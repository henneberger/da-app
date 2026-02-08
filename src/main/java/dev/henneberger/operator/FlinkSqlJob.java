package dev.henneberger.operator;

import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Kind;
import io.fabric8.kubernetes.model.annotation.Plural;
import io.fabric8.kubernetes.model.annotation.ShortNames;
import io.fabric8.kubernetes.model.annotation.Version;

@Group("dev.henneberger")
@Version("v1alpha1")
@Kind("FlinkSqlJob")
@Plural("flinksqljobs")
@ShortNames("fsql")
public class FlinkSqlJob extends CustomResource<FlinkSqlJobSpec, FlinkSqlJobStatus>
    implements Namespaced {}
