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
        self.adjusters = []
        PluginBase.__init__(self, **kwarg)
        self.init_adjuster_defaults()

    def init_adjusters_defaults(self):
        """
        adjusters: [{"minNodes": <minNodes>,
                     "maxNodes": <maxNodes>,
                     "minDurationSeconds": <minDurationSeconds>,
                     "maxDurationSeconds": <maxDurationSeconds>,
                     "nodesToDecrease": <nodesToDecrease>,
                     "durationSecondsToDecrease": <durationSecondsToDecrease>}]
        """
        adj_defaults = {"minNodes": 1,
                        "maxNodes": 10000,
                        "minDurationSeconds": 1800,
                        "maxDurationSeconds": 7200,
                        "nodesToDecrease": 1,
                        "durationSecondsToDecrease": 60}
        if self.adjusters:
            for adjuster in self.adjusters:
                for key, value in adj_defaults.items():
                    if key not in adjuster:
                        adjuster[key] = value

    # get backfill resources
    def get_bf_resources(self, blocking=False):
        # make logger
        tmpLog = self.make_logger(baseLogger, 'PBSBackfillWorkerAdjuster',
                                  method_name='get_bf_resources')

        resources = []
        # command
        if blocking:
            comStr = "showbf -p {0} --blocking".format(self.partition)
        else:
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
                        resources.append({'nodes': nodes, 'duration': duration})
                    except:
                        tmpLog.error("Failed to parse line: %s" % line)

        else:
            # failed
            errStr = stdOut + ' ' + stdErr
            tmpLog.error(errStr)
        tmpLog.info("Available backfill resources: %s" % resources)
        return resources

    def adjust_resources(self, resources):
        ret_resources = []
        for resource in resources:
            if resource['nodes'] > self.maxNodes:
                resource['nodes'] = self.maxNodes
            adjuster = None
            for adj in self.adjusters:
                if resource['nodes'] >= adj['minNodes'] and resource['nodes'] <= adj['maxNodes']:
                    adjuster = adj
                    break
            if adjuster:
                nodes = resource['nodes'] - adjuster['nodesToDecrease']
                duration = resource['duration']
                if duration == 'INFINITY':
                    duration = adjuster['maxDurationSeconds']
                else:
                    h, m, s = duration.split(':')
                    duration = int(h) * 3600 + int(m) * 60 + int(s)
                    if duration >= adjuster['minDurationSeconds'] and duration <= adjuster['maxDurationSeconds']:
                        duration -= adjuster['durationSecondsToDecrease']
                        ret_resources.append({'nodes': nodes, 'duration': duration, 'nCore': nodes * self.nCorePerNode})

        ret_resources.sort(key=lambda my_dict: my_dict['nodes'] * my_dict['duration'], reverse=True)
        return ret_resources

    def get_dynamic_resource(self, queue_name, resource_type):
        resources = self.get_bf_resources()
        if resources:
            resources = self.adjust_resources(resources)
            if resources:
                return {'nNewWorkers': 1, 'resources': resources}
        return {}
