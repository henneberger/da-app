package dev.henneberger.operator;

import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Kind;
import io.fabric8.kubernetes.model.annotation.Version;

@Group("dev.henneberger")
@Version("v1alpha1")
@Kind("FlinkPythonFunction")
public class FlinkPythonFunction extends CustomResource<FlinkPythonFunctionSpec, FlinkPythonFunctionStatus>
    implements Namespaced {
}
