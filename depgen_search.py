#! /usr/bin/env python
# coding:utf-8

from depgen import DepGen
import pymongo as pm


class DepGenSearch(DepGen):
    def __init__(
        self,
        host="localhost",
        port=27017,
        db="depgen",
        coll="twitter",
    ):
        DepGen.__init__(self)

        # mongo setup
        self.host = host
        self.port = port
        self.db = db
        self.coll = coll
        self.client = pm.MongoClient(self.host, self.port)
        self.colld = self.client[self.db][self.coll]

    def _mkpair(self, lst, deplst):
        ans = set()
        for ind, pair1 in enumerate(lst):
            for pair2 in deplst:
                s = set(pair1).intersection(pair2)
                if s and s != set(pair2):
                    a = tuple(sorted(set(pair1).union(pair2)))
                    ans.add(a)
        return ans

    def mkpair(self, lst, n):
        tmp = lst
        for i in range(n):
            tmp = self._mkpair(tmp, lst)
        return tmp

    def clause_pairs(self, tree) -> [dict]:
        """
        override
        """
        fltr = {"名詞", "動詞", "形容詞", "形容動詞"}
        return [item for item in DepGen.clause_pairs(self, tree) if
                item["left"]["pos"] in fltr or item["right"]["pos"] in fltr]

    def clause_one(self, tree) -> [dict]:
        """
        clause_pair が見つからなかったときに呼ぶ
        """
        lst = []
        for _id, chunk in enumerate(tree):
            clause = self.clause_info(chunk, _id)
            if clause["pos"] in {"名詞", "動詞", "形容詞", "形容動詞"}:
                left, right = clause, clause
                lst.append({
                    "left": left,
                    "right": right,
                })
        return lst

    def analyze_one(self, text) -> [dict]:
        tree = self.analyzer.parse(text)
        return self.clause_one(tree)

    def search(self, text, n=2):
        """
        n ステップで辿ることのできる文節ペアを検索
        """

        lst = self.analyze(text) or self.analyze_one(text)
        ret = []

        for dic in lst:
            left = dic["left"]
            right = dic["right"]

            # * key は除く
            if left["subject"] == "*" or right["subject"] == "*":
                continue

            print("depgen search key: {}, {}".format(
                left["subject"], right["subject"]
            ))
            # 文を検索
            for i, item in enumerate(self.colld.find(
                    {"left.subject": left["subject"],
                     "right.subject": right["subject"]
                     },
                    timeout=False,
                    limit=0
            )):
                text_id = item["id"]
                left_id = item["left"]["id"]
                right_id = item["right"]["id"]
                token_ids = [left_id, right_id]
                cands = []

                if left_id == right_id:
                    pairs = set()
                    id_dic = dict()
                else:
                    pairs = {(left_id, right_id)}
                    id_dic = {left_id: item["left"], right_id: item["right"]}

                for _ in range(n):
                    for i, next_item in enumerate(self.colld.find(
                            {"id": text_id,
                             "$or": [{"left.id": {"$in": token_ids}},
                                     {"right.id": {"$in": token_ids}}],
                             },
                            timeout=False,
                            limit=0
                    )):
                        pairs.add(
                            (next_item["left"]["id"], next_item["right"]["id"])
                        )
                        if next_item != item and next_item not in cands:
                            cands.append(next_item)
                            token_ids.extend([
                                next_item["left"]["id"],
                                next_item["right"]["id"]
                            ])

                            id_dic[next_item["left"]["id"]] = next_item["left"]
                            id_dic[next_item["right"]["id"]] = \
                                next_item["right"]

                for lst in self.mkpair(pairs, n):
                    if id_dic[lst[-1]]["pos"] in {
                            "動詞", "形容詞", "形容動詞"
                    }:
                        ret.append([id_dic[i] for i in lst])

        return ret

if __name__ == '__main__':
    import sys

    ds = DepGenSearch()

    cands = ds.search(sys.argv[1])
    for cand in cands:
        print(
            cand["left"]["surface"],
            cand["right"]["surface"]
        )
