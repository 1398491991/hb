#coding=utf-8
import os
from ..settings import BASE_DIR
import ConfigParser

class HbSpiderConfig:
    section = 'hb spider'
    def __init__(self,):
        self.path = os.path.join(BASE_DIR,*('hibor','spider_cfg','cfg','hb.cfg'))
        with open(self.path,'r') as fr:
            self.cfg = ConfigParser.ConfigParser()
            self.cfg.readfp(fr)

    def update(self,kv):
        for k,v in kv.items():
            self.cfg.set(self.section,k,v)

    def save(self):
        with open(self.path, 'w') as fw:
            self.cfg.write(fw)


