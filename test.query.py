import unittest2
from query import Query


class TestQueryClass(unittest2.TestCase):

    def test_query(self):

        query = Query()
        expected_result = {
            "right_owners": [
                {
                    "name": "VICTOR CORDERO AURRECOECHEA",
                    "role": "Compositor/Autor",
                    "ipi": "00006771296"
                },
                {
                    "name": "EDIT MEX DE MUSICA INT S A",
                    "role": "Editor",
                    "ipi": "00159586128"
                }
            ]
        }
        right_owners = query.find_right_owners("T0350190010")
        self.assertEqual(right_owners, expected_result)
