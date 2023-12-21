import unittest
import tempfile

from src.webcache import WebCache


class TestWebCache(unittest.TestCase):

    def test_webcache(self):
        with tempfile.TemporaryDirectory(prefix="investigate-news-test") as dir:
            cache = WebCache(
                path=f"{dir}/a/b/c",
            )

            response = cache.get("https://github.com/defgsus/investigate-news")
            self.assertEqual(1, cache.num_requests)

            self.assertEqual(
                [b'005ea0b48d52b931760928a717f5848f7afbfa3a0c33bba5f47fcd96db0621a75ef85751ba25c07578fa4fc79ed8e918'],
                [i[0] for i in cache.db.iterator()],
            )

            response2 = cache.get("https://github.com/defgsus/investigate-news")
            self.assertEqual(1, cache.num_requests)

            self.assertNotEqual(id(response), id(response2))
            self.assertEqual(response.content, response2.content)

