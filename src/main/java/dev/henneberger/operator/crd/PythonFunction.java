package dev.henneberger.operator.crd;

import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Version;
import io.fabric8.kubernetes.model.annotation.Kind;
import io.fabric8.kubernetes.api.model.Namespaced;

@Group("vertx.henneberger.dev")
@Version("v1alpha1")
@Kind("PythonFunction")
public class PythonFunction extends CustomResource<PythonFunctionSpec, PythonFunctionStatus> implements Namespaced {
}

