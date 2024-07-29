import jinja2
import yaml
import json


def main():
    model_info = {}
    # Import deployment config
    with open("deployment.yml") as f:
        conf = yaml.load(f, Loader=yaml.FullLoader).get("model_groups")

    with open("target/manifest.json") as f:
        manifest = json.load(f)

    # Flatten
    for node_id, node_info in manifest.get("nodes", {}).items():

        node_id = node_id.split(".")[-1]

        if (
            node_info.get("resource_type") == "model"
            and node_info.get("package_name") == "dbt_airflow_integration"
        ):
            model_info[node_id] = {}
            model_info[node_id]["group_name"] = node_info.get("path").split("/")[0]
            model_info[node_id]["inner_upstream"] = []
            model_info[node_id]["outer_upstream"] = []

    for node_id, node_info in manifest.get("nodes", {}).items():

        node_id = node_id.split(".")[-1]
        group_name = node_info.get("path").split("/")[0]

        if (
            node_info.get("resource_type") == "model"
            and node_info.get("package_name") == "dbt_airflow_integration"
        ):
            for upstream_node in node_info.get("depends_on").get("nodes"):
                if upstream_node.startswith("source."):
                    continue
                upstream_node = upstream_node.split(".")[-1]
                if model_info.get(upstream_node).get("group_name") == group_name:
                    model_info[node_id]["inner_upstream"].append(upstream_node)
                else:
                    model_info[node_id]["outer_upstream"].append(upstream_node)

    j_env = jinja2.Environment(loader=jinja2.FileSystemLoader("templates"))
    template = j_env.get_template("dag.py")
    for cfg in conf:
        cfg["nodes"] = {k: v for k, v in model_info.items() if v.get("group_name") == cfg.get("name")}
        r = template.render(**cfg)
        with open(f"dbt_dag__{cfg.get('name')}.py", "w") as f:
            f.write(r)


if __name__ == "__main__":
    main()
