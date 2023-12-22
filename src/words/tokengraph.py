import pickle
from pathlib import Path
from copy import deepcopy
from typing import Set, Iterable, Dict, Tuple, Optional, Callable, Union, BinaryIO

from .calcdict import CalcDict


class TokenGraph:

    DEFAULT_VERTEX = {
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
        return f"TokenGraph({len(self.vertices):,} x {len(self.edges):,})"

    def __copy__(self):
        return self.copy()

    def copy(self) -> "TokenGraph":
        instance = TokenGraph(directed=self.directed, allow_self_reference=self.allow_self_reference)
        instance.num_all_tokens = self.num_all_tokens
        instance.num_all_edges = self.num_all_edges
        instance.vertices = deepcopy(self.vertices)
        instance.edges = deepcopy(self.edges)
        return instance

    def to_pickle(self, file: Union[str, Path, BinaryIO]):
        data = {
            "directed": self.directed,
            "allow_self_reference": self.allow_self_reference,
            "num_all_tokens": self.num_all_tokens,
            "num_all_edges": self.num_all_edges,
            "vertices": self.vertices,
            "edges": self.edges,
        }
        if isinstance(file, (str, Path)):
            with open(file, "wb") as fp:
                pickle.dump(data, fp)
        else:
            pickle.dump(data, file)

    @classmethod
    def from_pickle(cls, file: Union[str, Path, BinaryIO]) -> "TokenGraph":
        if isinstance(file, (str, Path)):
            with open(file, "rb") as fp:
                data = pickle.load(fp)
        else:
            data = pickle.load(file)

        instance = cls(
            directed=data["directed"],
            allow_self_reference=data["allow_self_reference"],
        )
        instance.num_all_tokens = data["num_all_tokens"]
        instance.num_all_edges = data["num_all_edges"]
        instance.vertices = data["vertices"]
        instance.edges = data["edges"]
        return instance

    def add_related_tokens(self, tokens: Iterable[str]):
        if not isinstance(tokens, (tuple, list)):
            tokens = list(tokens)

        for token in tokens:
            if token not in self.vertices:
                vertex = self.DEFAULT_VERTEX.copy()
                self.vertices[token] = vertex
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
        min_v, max_v = self.vertices_count_min_max()
        min_e, max_e = self.edges_count_min_max()
        return {
            "vertices": len(self.vertices),
            "edges": len(self.edges),
            "vertices_count_min": min_v,
            "vertices_count_max": max_v,
            "edges_count_min": min_e,
            "edges_count_max": max_e,
        }

    def vertices_count_min_max(self) -> Tuple[int, int]:
        min_v, max_v = None, None
        for v in self.vertices.values():
            if min_v is None or v["count"] < min_v:
                min_v = v["count"]
            if max_v is None or v["count"] > max_v:
                max_v = v["count"]
        return min_v, max_v

    def edges_count_min_max(self) -> Tuple[int, int]:
        min_e, max_e = None, None
        for e in self.edges.values():
            if min_e is None or e["count"] < min_e:
                min_e = e["count"]
            if max_e is None or e["count"] > max_e:
                max_e = e["count"]
        return min_e, max_e

    def num_edges_per_vertex(self) -> Dict[str, int]:
        result = {key: 0 for key in self.vertices.keys()}
        for a, b in self.edges.keys():
            result[a] += 1
            result[b] += 1
        return result

    def vertex_counts(self) -> CalcDict:
        return CalcDict({
            k: v["count"]
            for k, v in self.vertices.items()
        })

    def edge_counts(self) -> CalcDict:
        return CalcDict({
            k: v["count"]
            for k, v in self.edges.items()
        })

    def vertex_frequencies(self) -> CalcDict:
        count_all = max(1, self.num_all_tokens)
        return CalcDict({
            k: v["count"] / count_all
            for k, v in self.vertices.items()
        })

    def edge_frequencies(self) -> CalcDict:
        count_all = max(1, self.num_all_edges)
        return CalcDict({
            k: v["count"] / count_all
            for k, v in self.edges.items()
        })

    def get_token_edges(self, token: str):
        return sorted(
            (
                (key, value) for key, value in self.edges.items()
                if key[0] == token or key[1] == token
            ),
            key=lambda x: x[1]["count"],
            reverse=True,
        )

    def filter(
            self,
            vertex_count_gte: Optional[int] = None,
            vertex_tokens: Optional[Iterable[str]] = None,
            vertex_function: Optional[Callable] = None,
            edges_per_vertex_gte: Optional[int] = None,
            edge_count_gte: Optional[int] = None,
            edge_tokens: Optional[Iterable[str]] = None,
            edge_function: Optional[Callable] = None,
            unconnected: bool = False,
            inplace: bool = False
    ) -> "TokenGraph":
        if inplace:
            instance = self
        else:
            instance = self.copy()

        if edge_count_gte is not None:
            instance.edges = {
                key: value
                for key, value in instance.edges.items()
                if value["count"] >= edge_count_gte
            }

        if edge_function is not None:
            instance.edges = {
                key: value
                for key, value in instance.edges.items()
                if edge_function(key, value)
            }

        if edge_tokens is not None:
            edge_tokens = set(edge_tokens)
            instance.edges = {
                key: value
                for key, value in instance.edges.items()
                if key[0] in edge_tokens or key[1] in edge_tokens
            }

        if vertex_count_gte is not None:
            instance.vertices = {
                key: value
                for key, value in instance.vertices.items()
                if value["count"] >= vertex_count_gte
            }

        if edges_per_vertex_gte is not None:
            counts = instance.num_edges_per_vertex()
            instance.vertices = {
                key: value
                for key, value in instance.vertices.items()
                if counts[key] >= edges_per_vertex_gte
            }

        if vertex_tokens is not None:
            vertex_tokens = set(vertex_tokens)
            instance.vertices = {
                key: value
                for key, value in instance.vertices.items()
                if key in vertex_tokens
            }

        if vertex_function is not None:
            instance.vertices = {
                key: value
                for key, value in instance.vertices.items()
                if vertex_function(key, value)
            }

        if unconnected:
            connected_vertices = set()
            for token1, token2 in instance.edges.keys():
                connected_vertices.add(token1)
                connected_vertices.add(token2)

            instance.vertices = {
                key: value
                for key, value in instance.vertices.items()
                if key in connected_vertices
            }

        instance.edges = {
            key: value
            for key, value in instance.edges.items()
            if key[0] in instance.vertices and key[1] in instance.vertices
        }

        instance.num_all_tokens = sum(v["count"] for v in instance.vertices.values())
        instance.num_all_edges = sum(v["count"] for v in instance.edges.values())

        return instance

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
            graph.vs["label"] = list(self.vertices.keys())

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
