import pickle
from pathlib import Path
from copy import deepcopy
from typing import Set, Iterable, Dict, Tuple, Optional, Callable, Union, BinaryIO, TextIO

from ..mixin import StateDictMixin
from .calcdict import CalcDict


class TokenGraph(StateDictMixin):

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

    def __contains__(self, token_or_edge: Union[str, Tuple[str, str]]):
        if isinstance(token_or_edge, str):
            return token_or_edge in self.vertices
        return token_or_edge in self.edges

    def copy(self) -> "TokenGraph":
        instance = TokenGraph(directed=self.directed, allow_self_reference=self.allow_self_reference)
        instance.num_all_tokens = self.num_all_tokens
        instance.num_all_edges = self.num_all_edges
        instance.vertices = deepcopy(self.vertices)
        instance.edges = deepcopy(self.edges)
        return instance

    def state_dict(self) -> dict:
        return {
            "directed": self.directed,
            "allow_self_reference": self.allow_self_reference,
            "num_all_tokens": self.num_all_tokens,
            "num_all_edges": self.num_all_edges,
            "vertices": self.vertices,
            "edges": self.edges,
        }

    @classmethod
    def from_state_dict(cls, data: dict):
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

    def remove_tokens(self, tokens: Iterable[str]):
        for tok in tokens:
            self.vertices.pop(tok, None)

        self._remove_edges_without_vertex()
        self._update_num_all()

    def info(self) -> dict:
        min_v, max_v = self.vertices_count_min_max()
        min_e, max_e = self.edges_count_min_max()
        min_d, max_d = self.degree_min_max()
        return {
            "vertices": len(self.vertices),
            "edges": len(self.edges),
            "vertices_count_min": min_v,
            "vertices_count_max": max_v,
            "edges_count_min": min_e,
            "edges_count_max": max_e,
            "degree_min": min_d,
            "degree_max": max_d,
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

    def degree_min_max(self) -> Tuple[int, int]:
        min_e, max_e = None, None
        for e in self.degree().values():
            if min_e is None or e < min_e:
                min_e = e
            if max_e is None or e > max_e:
                max_e = e
        return min_e, max_e

    def degree(self) -> CalcDict:
        return self._degree(self.vertices, self.edges, None)

    def degree_in(self) -> CalcDict:
        return self._degree(self.vertices, self.edges, None)

    def degree_out(self) -> CalcDict:
        return self._degree(self.vertices, self.edges, None)

    @classmethod
    def _degree(
            cls,
            vertices: Dict[str, dict],
            edges: Dict[Tuple[str, str], dict],
            in_out: Optional[bool],
    ) -> CalcDict:
        result = CalcDict({key: 0 for key in vertices.keys()})
        if in_out is None:
            for a, b in edges.keys():
                result[a] += 1
                result[b] += 1

        elif in_out is True:
            for a, b in edges.keys():
                result[b] += 1

        elif in_out is False:
            for a, b in edges.keys():
                result[a] += 1

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
            degree_gte: Optional[int] = None,
            edge_count_gte: Optional[int] = None,
            edge_tokens: Optional[Iterable[str]] = None,
            edge_function: Optional[Callable] = None,
            inplace: bool = False
    ) -> "TokenGraph":
        edges = self.edges
        vertices = self.vertices

        if edge_count_gte is not None:
            edges = {
                key: value
                for key, value in edges.items()
                if value["count"] >= edge_count_gte
            }

        if edge_function is not None:
            edges = {
                key: value
                for key, value in edges.items()
                if edge_function(key, value)
            }

        if edge_tokens is not None:
            edge_tokens = set(edge_tokens)
            edges = {
                key: value
                for key, value in edges.items()
                if key[0] in edge_tokens or key[1] in edge_tokens
            }

        if vertex_count_gte is not None:
            vertices = {
                key: value
                for key, value in vertices.items()
                if value["count"] >= vertex_count_gte
            }

        if degree_gte is not None:
            counts = self._degree(vertices, edges, None)
            vertices = {
                key: value
                for key, value in vertices.items()
                if counts[key] >= degree_gte
            }

        if vertex_tokens is not None:
            vertex_tokens = set(vertex_tokens)
            vertices = {
                key: value
                for key, value in vertices.items()
                if key in vertex_tokens
            }

        if vertex_function is not None:
            vertices = {
                key: value
                for key, value in vertices.items()
                if vertex_function(key, value)
            }

        edges = {
            key: value
            for key, value in edges.items()
            if key[0] in vertices and key[1] in vertices
        }

        if inplace:
            instance = self
        else:
            instance = TokenGraph(
                directed=self.directed,
                allow_self_reference=self.allow_self_reference,
            )

        instance.vertices = vertices
        instance.edges = edges
        instance._update_num_all()
        return instance

    def filter_repeat(
            self,
            vertex_count_gte: Optional[int] = None,
            vertex_tokens: Optional[Iterable[str]] = None,
            vertex_function: Optional[Callable] = None,
            degree_gte: Optional[int] = None,
            edge_count_gte: Optional[int] = None,
            edge_tokens: Optional[Iterable[str]] = None,
            edge_function: Optional[Callable] = None,
            max_repetions: Optional[int] = None,
            inplace: bool = False,
    ) -> "TokenGraph":
        fg1 = self
        count = 0
        while True:
            fg2 = fg1.filter(
                vertex_count_gte=vertex_count_gte,
                vertex_tokens=vertex_tokens,
                vertex_function=vertex_function,
                degree_gte=degree_gte,
                edge_count_gte=edge_count_gte,
                edge_tokens=edge_tokens,
                edge_function=edge_function,
                inplace=False,
            )
            count += 1
            if max_repetions is not None and count >= max_repetions:
                break
            if len(fg1.vertices) == len(fg2.vertices) and len(fg1.edges) == len(fg2.edges):
                break
            fg1 = fg2

        if not inplace:
            return fg2

        self.vertices = fg2.vertices
        self.edges = fg2.edges
        self.num_all_tokens = fg2.num_all_tokens
        self.num_all_edges = fg2.num_all_edges
        return self

    def _update_num_all(self):
        self.num_all_tokens = sum(v["count"] for v in self.vertices.values())
        self.num_all_edges = sum(v["count"] for v in self.edges.values())

    def _remove_edges_without_vertex(self):
        self.edges = {
            key: value
            for key, value in self.edges.items()
            if key[0] in self.vertices and key[1] in self.vertices
        }

    def dump(
            self,
            limit: int = 50,
            sort_key: Optional[Callable] = None,
            reverse: bool = True,
            file: Optional[TextIO] = None,
    ):
        info = self.info()
        max_len = max(len(key) for key in info.keys())
        for key, value in info.items():
            if isinstance(value, int):
                value = f"{value:,}"
            print(f"{key:{max_len}}: {value}", file=file)
        print("vertex frequencies:", file=file)
        self.vertex_frequencies().dump(limit=limit, sort_key=sort_key, reverse=reverse, file=file)
        print("edge frequencies:", file=file)
        self.edge_frequencies().dump(limit=limit, sort_key=sort_key, reverse=reverse, file=file)

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
