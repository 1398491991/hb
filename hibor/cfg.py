#coding=utf-8
import ConfigParser
import datetime
from .settings import BASE_DIR
import os

class SpiderConfig:

    def __init__(self,):
        self.path = os.path.join(BASE_DIR,'hibor/local_settings.ini')
        with open(self.path,'r') as fr:
            self.cfg = ConfigParser.ConfigParser()
            self.cfg.readfp(fr)

    def getint(self,section, option,default_value=0):
        try:
            return self.cfg.getint(section, option)
        except ValueError:
            return default_value

    def set(self,section, option, value=None):
        return self.cfg.set(section, option, value=value)

    def getitems(self,section, raw=False, vars=None):
        return self.cfg.items(section, raw=raw, vars=vars)

    def update(self,continuous_page,start_page,finish_type,section='spider config',
               is_save=True):
        update_count = self.getint(section,'update_count')+1
        update_time = str(datetime.datetime.now())
        self.set(section,'update_count',update_count)
        self.set(section,'update_time',update_time)
        self.set(section,'continuous_page',continuous_page)
        self.set(section,'finish_type',finish_type)
        self.set(section,'start_page',start_page)
        if is_save:
            self.save()

    def save(self):
        with open(self.path, 'w') as fw:
            self.cfg.write(fw)











