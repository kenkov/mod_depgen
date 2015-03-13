#! /usr/bin/env python
# coding:utf-8


from mod import Mod
from logging import getLogger
from depgen_search import DepGenSearch
import numpy as np
import mod_depgen_config as config


class ModDepgen(Mod):
    def __init__(
        self,
        logger=None,
        host=config.hostname,
        port: int=config.port,
        db: str=config.db,
        coll: str=config.coll,
    ):
        self.logger = logger if logger else getLogger(__file__)
        self.ds = DepGenSearch(
            host=host,
            port=port,
            db=db,
            coll=coll
        )

    def utter(self, message, master):
        text = message["text"]
        cands = self.ds.search(text)

        return [(
            np.random.dirichlet((80, 20), 1)[0].max(),
            "".join([clause["surface"] for clause in cand]),
            "depgen",
            dict()
        ) for cand in cands]
