import unittest
from io import BytesIO

from src.words import TokenGraph


class TestTokenGraph(unittest.TestCase):

    def test_100_graph(self):
        tg = TokenGraph()
        tg.add_related_tokens(["a", "b", "b", "c", "c", "c"])
        self.assertEqual(
            {'a': 1, 'b': 2, 'c': 3},
            tg.vertex_counts(),
        )
        self.assertEqual(
            {('a', 'b'): 2, ('a', 'c'): 3, ('b', 'c'): 6},
            tg.edge_counts(),
        )
        self.assertEqual(
            {'a': 0.16666666666666666, 'b': 0.3333333333333333, 'c': 0.5},
            tg.vertex_frequencies(),
        )
        self.assertEqual(
            {('a', 'b'): 0.18181818181818182, ('a', 'c'): 0.2727272727272727, ('b', 'c'): 0.5454545454545454},
            tg.edge_frequencies(),
        )

        self.assertEqual(
            {'b': 2, 'c': 3},
            tg.filter(vertex_count_gte=2).vertex_counts(),
        )
        self.assertEqual(
            {('b', 'c'): 6},
            tg.filter(vertex_count_gte=2).edge_counts(),
        )

        self.assertEqual(
            {'a': 1, 'b': 2, 'c': 3},
            tg.filter(edge_count_gte=3).vertex_counts(),
        )
        self.assertEqual(
            {('a', 'c'): 3, ('b', 'c'): 6},
            tg.filter(edge_count_gte=3).edge_counts(),
        )

        self.assertEqual(
            {('a', 'b'): 2, ('a', 'c'): 3},
            tg.filter(edge_tokens=['a']).edge_counts(),
        )
        self.assertEqual(
            {('a', 'b'): .4, ('a', 'c'): .6},
            tg.filter(edge_tokens=['a']).edge_frequencies(),
        )
        self.assertEqual(
            {('a', 'b'): 2},
            tg.filter(vertex_tokens=['a', 'b']).edge_counts(),
        )
        self.assertEqual(
            {('a', 'b'): 1.},
            tg.filter(vertex_tokens=['a', 'b']).edge_frequencies(),
        )

        file = BytesIO()
        tg.to_pickle(file)

        file.seek(0)
        tg2 = TokenGraph.from_pickle(file)

        self.assertEqual(tg.vertices, tg2.vertices)
        self.assertEqual(tg.edges, tg2.edges)
        self.assertEqual(tg.edge_frequencies(), tg2.edge_frequencies())
