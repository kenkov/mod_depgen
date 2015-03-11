#! /usr/bin/env python
# coding:utf-8


from mod import Mod
from logging import getLogger
from depgen_search import DepGenSearch
import numpy as np


class ModDepgen(Mod):
    def __init__(
        self,
        logger=None,
        # host="localhost",
        # port: int=27017,
        # db: str="cabocha",
        # coll: str="cases",
    ):
        self.logger = logger if logger else getLogger(__file__)
        self.ds = DepGenSearch()

    def reses(self, message, master):
        text = message["text"]
        cands = self.ds.search(text)

        return [(
            np.random.dirichlet((80, 20), 1)[0].max(),
            "".join([clause["surface"] for clause in cand]),
            "depgen",
            dict()
        ) for cand in cands]
