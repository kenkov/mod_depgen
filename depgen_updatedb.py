#! /usr/bin/env python
# coding:utf-8


import sys
import pymongo as pm
from depgen import DepGen
from logging import getLogger
import mod_depgen_config as config


class DepgenUpdateDB:
    def __init__(
        self,
        logger=None,
        host=config.hostname,
        port: int=config.port,
        db: str=config.db,
        coll: str=config.coll,
    ):
        self.logger = logger if logger else getLogger(__file__)

        client = pm.MongoClient(host, port)
        self.colld = client[db][coll]
        self.dg = DepGen()

    def update(self, text):
        clause_pairs = self.dg.analyze(text)
        # find next id
        try:
            # 新しい id をみつける
            _id = list(self.colld.find(
                timeout=False,
                limit=0
            ).sort("id", pm.DESCENDING).limit(1))[0]["id"] + 1
        except:
            _id = 0
            if clause_pairs:
                self.logger.info("set initial id 0")
        for clause_pair in clause_pairs:
            clause_pair.update({"id": _id})
            self.colld.insert(clause_pair)
            self.logger.info("insert new pair: {} {} {}".format(
                _id,
                clause_pair["left"]["surface"], clause_pair["right"]["surface"]
            ))

        return False

    def reses(self, message, master):
        return []


if __name__ == '__main__':
    import argparse

    # parse arg
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "infile",
        nargs="?",
        type=argparse.FileType("r"),
        default=sys.stdin,
        help="target name"
    )
    args = parser.parse_args()
    dg = DepgenUpdateDB()

    for _id, text in enumerate(_.strip() for _ in args.infile):
        dg.update(text)
        if (_id + 1) % 1000 == 0:
            print("{} processed".format(_id + 1))
