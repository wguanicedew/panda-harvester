import json
import shutil
import datetime
import requests
import requests.exceptions

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy
from pandaharvester.harvesterbody.agent_base import AgentBase

# logger
_logger = core_utils.setup_logger('cacher')


# cache information
class Cacher(AgentBase):
    # constructor
    def __init__(self, communicator, single_mode=False):
        AgentBase.__init__(self, single_mode)
        self.dbProxy = DBProxy()
        self.communicator = communicator

    # main loop
    def run(self):
        while True:
            # execute
            self.execute()
            # check if being terminated
            if self.terminated(harvester_config.cacher.sleepTime, randomize=False):
                return

    # main
    def execute(self, force_update=False, skip_lock=False):
        mainLog = self.make_logger(_logger, 'id={0}'.format(self.get_pid()), method_name='execute')
        # get lock
        locked = self.dbProxy.get_process_lock('cacher', self.get_pid(), harvester_config.cacher.sleepTime)
        if locked or skip_lock:
            mainLog.debug('getting information')
            timeLimit = datetime.datetime.utcnow() - \
                datetime.timedelta(minutes=harvester_config.cacher.refreshInterval)
            itemsList = []
            keysForceUpdate = []
            nItems = 4
            for tmpStr in harvester_config.cacher.data:
                tmpItems = tmpStr.split('|')
                if len(tmpItems) < 3:
                    continue
                tmpItems += [None] * (nItems - len(tmpItems))
                tmpItems = tmpItems[:nItems]
                itemsList.append(tmpItems)
            # add queues_config
            if core_utils.get_queues_config_url() is not None:
                tmpKey = 'queues_config_file'
                itemsList.append((tmpKey, None, core_utils.get_queues_config_url()))
                keysForceUpdate.append(tmpKey)
            # loop over all items
            for mainKey, subKey, infoURL, dumpFile in itemsList:
                if subKey == '':
                    subKey = None
                # check last update time
                lastUpdateTime = self.dbProxy.get_cache_last_update_time(mainKey, subKey)
                if (not force_update or mainKey not in keysForceUpdate) and lastUpdateTime is not None \
                        and lastUpdateTime > timeLimit:
                    continue
                # get information
                tmpStat, newInfo = self.get_data(infoURL, mainLog)
                if not tmpStat:
                    mainLog.error('failed to get info for key={0} subKey={1}'.format(mainKey, subKey))
                    continue
                # update
                tmpStat = self.dbProxy.refresh_cache(mainKey, subKey, newInfo)
                if tmpStat:
                    mainLog.debug('refreshed key={0} subKey={1}'.format(mainKey, subKey))
                    if dumpFile is not None:
                        try:
                            tmpFileName = dumpFile + '.tmp'
                            with open(tmpFileName, 'w') as tmpFile:
                                json.dump(newInfo, tmpFile)
                            shutil.move(tmpFileName, dumpFile)
                        except Exception:
                            core_utils.dump_error_message(mainLog)
                else:
                    mainLog.error('failed to refresh key={0} subKey={1} due to a DB error'.format(mainKey, subKey))
            mainLog.debug('done')


    # get new data
    def get_data(self, info_url, tmp_log):
        retStat = False
        retVal = None
        if info_url.startswith('file:'):
            try:
                with open(info_url.split(':')[-1], 'r') as infoFile:
                    retVal = infoFile.read()
                    try:
                        retVal = json.loads(retVal)
                    except Exception:
                        pass
            except Exception:
                core_utils.dump_error_message(tmp_log)
        elif info_url.startswith('http'):
            try:
                res = requests.get(info_url, timeout=60)
                if res.status_code == 200:
                    try:
                        retVal = res.json()
                    except Exception:
                        errMsg = 'corrupted json from {0} : {1}'.format(info_url, res.text)
                        tmp_log.error(errMsg)
                else:
                    errMsg = 'failed to get {0} with StatusCode={1} {2}'.format(info_url, res.status_code, res.text)
                    tmp_log.error(errMsg)
            except requests.exceptions.ReadTimeout:
                tmp_log.error('read timeout when getting data from {0}'.format(info_url))
            except Exception:
                core_utils.dump_error_message(tmp_log)
        elif info_url.startswith('panda_cache:'):
            try:
                publicKey, privateKey = info_url.split(':')[-1].split('&')
                retVal, outStr = self.communicator.get_key_pair(publicKey, privateKey)
                if retVal is None:
                    tmp_log.error(outStr)
            except Exception:
                core_utils.dump_error_message(tmp_log)
        elif info_url.startswith('panda_server:'):
            try:
                retVal, outStr = self.communicator.get_resource_types()
                if not retVal:
                    tmp_log.error(outStr)
            except Exception:
                core_utils.dump_error_message(tmp_log)
        else:
            errMsg = 'unsupported protocol for {0}'.format(info_url)
            tmp_log.error(errMsg)
        if retVal is not None:
            retStat = True
        return retStat, retVal

    # set single mode
    def set_single_mode(self, single_mode):
        self.singleMode = single_mode
