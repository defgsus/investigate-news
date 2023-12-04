from typing import Set, Iterable, Dict, Tuple, Optional, Callable


class TokenGraph:

    DEFAULT_VERTEX = {
        "name": "",
        "count": 0,
    }

    DEFAULT_EDGE = {
        "count": 0,
    }

    def __init__(
            self,
            directed: bool = False,
            allow_self_reference: bool = False,
    ):
        self.directed = bool(directed)
        self.allow_self_reference = bool(allow_self_reference)
        self.vertices: Dict[str, dict] = {}
        self.edges: Dict[Tuple[str, str], dict] = {}
        self.num_all_tokens = 0
        self.num_all_edges = 0

    def __str__(self):
        return f"TokenGraph({len(self.vertices):,d} x {len(self.edges):,d})"

    def add_related_tokens(self, tokens: Iterable[str]):
        if not isinstance(tokens, (tuple, list)):
            tokens = list(tokens)

        for token in tokens:
            if token not in self.vertices:
                vertex = self.DEFAULT_VERTEX.copy()
                self.vertices[token] = vertex
                vertex["name"] = token
            else:
                vertex = self.vertices[token]

            vertex["count"] += 1
            self.num_all_tokens += 1

        for idx1, token1 in enumerate(tokens):
            for idx2 in range(idx1, len(tokens)):
                token2 = tokens[idx2]

                if not self.allow_self_reference and token1 == token2:
                    continue

                edge_key = (token1, token2)
                if not self.directed:
                    edge_key = tuple(sorted(edge_key))

                if edge_key not in self.edges:
                    edge = self.DEFAULT_EDGE.copy()
                    self.edges[edge_key] = edge
                else:
                    edge = self.edges[edge_key]

                edge["count"] += 1
                self.num_all_edges += 1

    def info(self) -> dict:
        min_v, max_v = None, None
        for v in self.vertices.values():
            if min_v is None or v["count"] < min_v:
                min_v = v["count"]
            if max_v is None or v["count"] > max_v:
                max_v = v["count"]
        min_e, max_e = None, None
        for e in self.edges.values():
            if min_e is None or e["count"] < min_e:
                min_e = e["count"]
            if max_e is None or e["count"] > max_e:
                max_e = e["count"]
        return {
            "vertices": len(self.vertices),
            "edges": len(self.edges),
            "vertices_count_min": min_v,
            "vertices_count_max": max_v,
            "edges_count_min": min_e,
            "edges_count_max": max_e,
        }

    def num_edges_per_vertex(self) -> Dict[str, int]:
        result = {key: 0 for key in self.vertices.keys()}
        for a, b in self.edges.keys():
            result[a] += 1
            result[b] += 1
        return result

    def vertex_frequencies(self) -> Dict[str, float]:
        count_all = max(1, self.num_all_tokens)
        return {
            k: v["count"] / count_all
            for k, v in self.vertices.items()
        }

    def edge_frequencies(self) -> Dict[str, float]:
        count_all = max(1, self.num_all_edges)
        return {
            k: v["count"] / count_all
            for k, v in self.edges.items()
        }

    def drop_edges(
            self,
            count__lt: Optional[int] = None,
            function: Optional[Callable] = None,
    ) -> None:
        if count__lt is not None:
            self.edges = {
                key: value
                for key, value in self.edges.items()
                if count__lt < value["count"]
            }

        if function is not None:
            self.edges = {
                key: value
                for key, value in self.edges.items()
                if not function(key, value)
            }

    def drop_vertices(
            self,
            num_edges__lt: Optional[int] = None,
            unconnected: bool = False,
            function: Optional[Callable] = None,
    ) -> None:
        if num_edges__lt is not None:
            counts = self.num_edges_per_vertex()
            self.vertices = {
                key: value
                for key, value in self.vertices.items()
                if num_edges__lt < counts[key]
            }

        if unconnected:
            connected_vertices = set()
            for token1, token2 in self.edges.keys():
                connected_vertices.add(token1)
                connected_vertices.add(token2)

            self.vertices = {
                key: value
                for key, value in self.vertices.items()
                if key in connected_vertices
            }

        if function is not None:
            self.vertices = {
                key: value
                for key, value in self.vertices.items()
                if not function(key, value)
            }

        self.edges = {
            key: value
            for key, value in self.edges.items()
            if key[0] in self.vertices and key[1] in self.vertices
        }

    def to_igraph(self):
        import igraph

        def _check_edge_key(key: Tuple[str, str]):
            ok = True
            if key[0] not in self.vertices:
                ok = False
                print(f"edge key {key[0]} not in vertices!")
            if key[1] not in self.vertices:
                ok = False
                print(f"edge key {key[1]} not in vertices!")
            return ok

        graph = igraph.Graph(directed=self.directed)

        if self.vertices:
            graph.add_vertices(
                len(self.vertices),
                {
                    key: [
                        "None" if n[key] is None else n[key]
                        for n in self.vertices.values()
                    ]
                    for key in self.DEFAULT_VERTEX.keys()
                }
            )
            graph.vs["label"] = [
                n["name"]
                for n in self.vertices.values()
            ]

        if self.edges:
            graph.add_edges(
                [(e[0], e[1]) for e in self.edges.keys() if _check_edge_key(e)],
                {
                    key: [
                        "None" if n[key] is None else n[key]
                        for edge_key, n in self.edges.items()
                        if _check_edge_key(edge_key)
                    ]
                    for key in self.DEFAULT_EDGE.keys()
                }
            )

            #max_weight = max(*graph.es["weight"])
            #if max_weight:
            #    graph.es["weight"] = [max(self.MIN_WEIGHT, round(w / max_weight, 4)) for w in graph.es["weight"]]
            #else:
            #    graph.es["weight"] = [self.MIN_WEIGHT] * len(graph.es)

        return graph

    def to_graphviz(self):
        import graphviz
        g = graphviz.Graph()
        for edge in self.edges.keys():
            g.edge(edge[0], edge[1])
        return g
