try:
    import subprocess32 as subprocess
except:
    import subprocess

from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore import core_utils


# PBS backfill adjuster

# logger
baseLogger = core_utils.setup_logger('pbs_backfill_worker_adjuster')


class PBSBackfillWorkerAdjuster(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

    # get backfill resources
    def get_bf_resources(self):
        # make logger
        tmpLog = self.make_logger(baseLogger, 'PBSBackfillWorkerAdjuster',
                                  method_name='get_bf_resources')

        resources = {}
        # command
        comStr = "showbf -p {0}".format(self.partition)
        # get backfill resources
        tmpLog.debug('Get backfill resources with {0}'.format(comStr))
        p = subprocess.Popen(comStr.split(),
                             shell=False,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        # check return code
        stdOut, stdErr = p.communicate()
        retCode = p.returncode
        tmpLog.debug('retCode={0}'.format(retCode))
        if retCode == 0:
            # extract batchID
            tmpLog.debug("Available backfill resources for partition(%s):\n%s" % (self.partition, stdOut))
            lines = stdOut.splitlines()
            for line in lines:
                line = line.strip()
                if line.startswith(self.partition):
                    try:
                        items = line.split()
                        nodes = int(items[2])
                        if nodes < self.minNodes:
                            continue
                        duration = items[3]
                        if duration == 'INFINITY':
                            duration = self.defaultDurationSeconds
                        else:
                            h, m, s = duration.split(':')
                            duration = int(h) * 3600 + int(m) * 60 + int(s)
                            if duration < self.minDurationSeconds:
                                continue
                        key = nodes * duration
                        if key not in resources:
                            resources[key] = []
                        resources[key].append({'nodes': nodes, 'duration': duration})
                    except:
                        tmpLog.error("Failed to parse line: %s" % line)

        else:
            # failed
            errStr = stdOut + ' ' + stdErr
            tmpLog.error(errStr)
        tmpLog.info("Available backfill resources: %s" % resources)
        return resources

    def get_dynamic_resource(self, queue_name, resource_type):
        resources = self.get_bf_resources()
        if resources:
            # TODO, add nCores or nNodes to it too for non-ES
            return {'nNewWorkers': 1}
        return None
