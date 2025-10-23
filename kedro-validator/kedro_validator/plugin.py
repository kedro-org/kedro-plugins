from typing import Any, Dict, get_type_hints
import inspect
import dataclasses
from kedro.framework.hooks import hook_impl

class KedroValidationHook:
    """Validate params against dataclasses or Pydantic models referenced by node functions."""

    @hook_impl
    def after_context_created(self, context) -> None:
        """
        Runs early — context.params is available.
        It inspects pipeline nodes, finds inputs of the form "params:<key>"
        and validates them against function annotations.
        """
        params: Dict[str, Any] = context.params
        try:
            from kedro.framework.project import pipelines
        except Exception:
            pipelines = {}

        problems = []

        for pipeline_name, pipeline in (pipelines.items() if hasattr(pipelines, "items") else getattr(pipelines, "__dict__", {}).items()):
            for node in getattr(pipeline, "nodes", []):
                func = getattr(node, "func", None)
                if func is None:
                    continue
                sig = inspect.signature(func)
                type_hints = get_type_hints(func, include_extras=False)

                # Node.inputs can be a list or dict mapping.
                # mapping dataset_name -> arg_name (best-effort).
                dataset_to_arg = {}
                node_inputs = getattr(node, "inputs", None)
                if isinstance(node_inputs, dict):
                    # mapping dataset_name -> arg name
                    dataset_to_arg.update(node_inputs)
                elif isinstance(node_inputs, (list, tuple)):
                    # map by position to function params
                    param_names = list(sig.parameters.keys())
                    for idx, ds in enumerate(node_inputs):
                        if idx < len(param_names):
                            dataset_to_arg[ds] = param_names[idx]
                # else: leave empty

                # Inspect params:* dataset inputs
                for ds_name, arg_name in dataset_to_arg.items():
                    if not isinstance(ds_name, str):
                        continue
                    if not ds_name.startswith("params:"):
                        continue

                    print("Param DS Name", ds_name)
                    # extract param key (strip prefix)
                    param_key = ds_name.split(":", 1)[1]
                    # support nested lookups like "params:train" -> context.params['train']
                    raw_value = params.get(param_key)
                    if raw_value is None:
                        # If param absent, collect problem (could be optional)
                        problems.append({
                            "node": node.name or getattr(func, "__name__", "<unknown>"),
                            "pipeline": pipeline_name,
                            "param_key": param_key,
                            "error": "missing parameter"
                        })
                        continue

                    # find expected type for arg_name
                    expected = type_hints.get(arg_name)
                    if expected is None:
                        # no annotation — skip or optionally warn
                        continue

                    # handle pydantic models
                    try:
                        import pydantic
                        BaseModel = pydantic.BaseModel
                    except Exception:
                        BaseModel = None

                    try:
                        if BaseModel and inspect.isclass(expected) and issubclass(expected, BaseModel):
                            # instantiate/validate
                            try:
                                expected.model_validate(raw_value)
                            except Exception as exc:
                                problems.append({
                                    "node": node.name or getattr(func, "__name__", "<unknown>"),
                                    "pipeline": pipeline_name,
                                    "param_key": param_key,
                                    "error": f"pydantic validation failed: {exc}"
                                })
                        elif dataclasses.is_dataclass(expected):
                            # For dataclasses: try to instantiate
                            try:
                                expected(**(raw_value if isinstance(raw_value, dict) else {}))
                            except Exception as exc:
                                problems.append({
                                    "node": node.name or getattr(func, "__name__", "<unknown>"),
                                    "pipeline": pipeline_name,
                                    "param_key": param_key,
                                    "error": f"dataclass instantiation failed: {exc}"
                                })
                        else:
                            # typed dicts or plain dicts only for static checking.
                            # [TODO: Not sure if the plugin should do anything here]
                            pass
                    except Exception as exc:
                        problems.append({
                            "node": node.name or getattr(func, "__name__", "<unknown>"),
                            "pipeline": pipeline_name,
                            "param_key": param_key,
                            "error": f"unexpected error during validation: {exc}"
                        })

        if problems:
            # Format a message and raise so Kedro CLI fails fast
            lines = ["Parameter validation failed:"]
            for p in problems:
                lines.append(f"- pipeline={p['pipeline']} node={p['node']} param={p['param_key']}: {p['error']}")
            raise RuntimeError("\n".join(lines))

validator_hook = KedroValidationHook()
