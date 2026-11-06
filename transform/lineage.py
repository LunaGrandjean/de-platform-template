"""Rewrite the generated block of docs/lineage.md from dbt's manifest.

    cd transform
    uv run --locked dbt docs generate --profiles-dir .
    python lineage.py

Only the text between the two markers is replaced; the prose around it is
hand-written and stays. Standard library only, so it needs no environment.
"""
import json
import re
from pathlib import Path

HERE = Path(__file__).resolve().parent
MANIFEST = HERE / "target" / "manifest.json"
DOC = HERE.parent / "docs" / "lineage.md"
START, END = "<!-- lineage:start -->", "<!-- lineage:end -->"
LAYERS = [("sources", "Bronze (sources)"), ("staging", "Silver: staging"),
          ("intermediate", "Silver: intermediate"), ("marts", "Gold: marts")]


def node_key(unique_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]", "_", unique_id)


def main() -> None:
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    nodes = {}  # unique_id -> (label, layer)
    for uid, src in manifest["sources"].items():
        nodes[uid] = (f"{src['source_name']}.{src['name']}", "sources")
    for uid, node in manifest["nodes"].items():
        if node["resource_type"] == "model":
            nodes[uid] = (node["name"], node["fqn"][1])
    edges = []
    for uid, node in manifest["nodes"].items():
        if node["resource_type"] != "model":
            continue
        for parent in node["depends_on"]["nodes"]:
            if parent in nodes:
                edges.append((parent, uid))

    lines = ["```mermaid", "flowchart LR"]
    for layer, title in LAYERS:
        lines.append(f'  subgraph {layer}["{title}"]')
        for uid, (label, node_layer) in sorted(nodes.items(), key=lambda kv: kv[1][0]):
            if node_layer == layer:
                lines.append(f'    {node_key(uid)}["{label}"]')
        lines.append("  end")
    for parent, child in sorted(edges):
        lines.append(f"  {node_key(parent)} --> {node_key(child)}")
    lines.append("```")

    children = {}
    for parent, child in edges:
        children.setdefault(parent, set()).add(child)

    def downstream(uid, seen=None):
        seen = set() if seen is None else seen
        for child in children.get(uid, ()):
            if child not in seen:
                seen.add(child)
                downstream(child, seen)
        return seen

    lines += ["", "| Source | Models downstream | Gold models reached |", "|---|---:|---|"]
    for uid, (label, layer) in sorted(nodes.items(), key=lambda kv: kv[1][0]):
        if layer != "sources":
            continue
        reached = downstream(uid)
        marts = sorted(nodes[r][0] for r in reached if nodes[r][1] == "marts")
        lines.append(f"| `{label}` | {len(reached)} | {', '.join(f'`{m}`' for m in marts) or 'none'} |")

    block = "\n".join(lines)
    text = DOC.read_text(encoding="utf-8")
    new = re.sub(re.escape(START) + r".*?" + re.escape(END), lambda _: f"{START}\n{block}\n{END}", text, flags=re.S)
    DOC.write_text(new, encoding="utf-8")
    print(f"{len([n for n in nodes.values() if n[1] != 'sources'])} models, "
          f"{len([n for n in nodes.values() if n[1] == 'sources'])} sources, {len(edges)} edges")


if __name__ == "__main__":
    main()
