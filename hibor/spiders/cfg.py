#coding=utf-8
import ConfigParser


class SpiderConfig:
    def __init__(self):
        with open('../local_settings.ini','r') as f:
            self.cfg = ConfigParser.ConfigParser()
            self.cfg.readfp(f)
    def getint(self,section, option):
        return self.cfg.getint(section, option)

    def getitems(self,section, raw=False, vars=None):
        return self.cfg.items(section, raw=raw, vars=vars)


