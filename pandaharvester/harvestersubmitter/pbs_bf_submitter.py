import datetime
import tempfile
try:
    import subprocess32 as subprocess
except:
    import subprocess

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvesterworkeradjuster.pbs_bf_worker_adjuster import PBSBackfillWorkerAdjuster

# logger
baseLogger = core_utils.setup_logger('pbs_bf_submitter')


# backfill submitter for PBS batch system
class PBSBackfillSubmitter(PBSBackfillWorkerAdjuster):
    # constructor
    def __init__(self, **kwarg):
        self.uploadLog = False
        self.logBaseURL = None
        self.nodesToDecrease = 0
        self.durationSecondsToDecrease = 0
        super(PBSBackfillSubmitter, self).__init__(self, **kwarg)
        # template for batch script
        tmpFile = open(self.templateFile)
        self.template = tmpFile.read()
        tmpFile.close()

    # adjust cores based on available events
    def get_needed_resource(self, workspec):
        tmpLog = self.make_logger(baseLogger, 'workerID={0}'.format(workSpec.workerID),
                                      method_name='get_needed_resource')
        if not hasattr(self, 'nSecondsPerEvent') or self.nSecondsPerEvent < 100:
            tmpLog.warn("nSecondsPerEvent is not set, will use default value 480 seconds(8 minutes)")
            nSecondsPerEvent = 480
        else:
            nSecondsPerEvent = self.nSecondsPerEvent

        nRemainingEvents = workspec.get_num_remaining_events()
        if nRemainingEvents <= 0:
            tmpLog.warn("Maybe nRemainingEvents is not correctly propagated or delayed, will not submit big jobs")

        resource = self.get_max_bf_resources(workspec)
        if resource:
            tmpLog.debug("Selected resources: %s" % resource)
            duration = resource['duration']
            if resource['nodes'] < self.defaultNodes:
                workspec.nCore = resource['nodes'] * self.nCorePerNode
            else:
                if nRemainingEvents <= 0:
                    tmpLog.warn("nRemainingEvents is not correctly propagated or delayed, will not submit big jobs, shrink number of nodes to default")
                    workspec.nCore = self.defaultNodes * self.nCorePerNode
                else:
                    neededNodes = nRemainingEvents * nSecondsPerEvent * 1.0 / resource['duration'] / self.nCorePerNode
                    neededNodes = int(math.ceil(neededNodes))
                    workspec.nCore = neededNodes * self.nCorePerNode
        else:
            workspec.nCore = self.defaultNodes * self.nCorePerNode
            duration = self.defaultDurationSeconds

        return workspec, duration
    
    # submit workers
    def submit_workers(self, workspec_list):
        retList = []
        for workSpec in workspec_list:
            # make logger
            tmpLog = self.make_logger(baseLogger, 'workerID={0}'.format(workSpec.workerID),
                                      method_name='submit_workers')

            workspec, duration = self.get_needed_resource(workSpec)
            
            # make batch script
            batchFile = self.make_batch_script(workSpec, duration)
            # command
            comStr = "qsub {0}".format(batchFile)
            # submit
            tmpLog.debug('submit with {0}'.format(comStr))
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
                workSpec.batchID = stdOut.split()[-1]
                tmpLog.debug('batchID={0}'.format(workSpec.batchID))
                # set log files
                if self.uploadLog:
                    if self.logBaseURL is None:
                        baseDir = workSpec.get_access_point()
                    else:
                        baseDir = self.logBaseURL
                    stdOut, stdErr = self.get_log_file_names(batchFile, workSpec.batchID)
                    if stdOut is not None:
                        workSpec.set_log_file('stdout', '{0}/{1}'.format(baseDir, stdOut))
                    if stdErr is not None:
                        workSpec.set_log_file('stderr', '{0}/{1}'.format(baseDir, stdErr))
                tmpRetVal = (True, '')
            else:
                # failed
                errStr = stdOut + ' ' + stdErr
                tmpLog.error(errStr)
                tmpRetVal = (False, errStr)
            retList.append(tmpRetVal)
        return retList

    # make batch script
    def make_batch_script(self, workspec, duration):
        # make logger
        tmpLog = self.make_logger(baseLogger, 'workerID={0}'.format(workspec.workerID),
                                  method_name='make_batch_script')

        duration = str(datetime.timedelta(seconds=duration))
        tmpFile = tempfile.NamedTemporaryFile(delete=False, suffix='_submit.sh', dir=workspec.get_access_point())
        tmpFile.write(self.template.format(nCorePerNode=self.nCorePerNode,
                                           localQueue=self.localQueue,
                                           projectName=self.projectName,
                                           nNode=workspec.nCore / self.nCorePerNode,
                                           accessPoint=workspec.accessPoint,
                                           duration=duration,
                                           workerID=workspec.workerID)
                      )
        tmpFile.close()
        return tmpFile.name

    # get log file names
    def get_log_file_names(self, batch_script, batch_id):
        stdOut = None
        stdErr = None
        with open(batch_script) as f:
            for line in f:
                if not line.startswith('#PBS'):
                    continue
                items = line.split()
                if '-o' in items:
                    stdOut = items[-1].replace('$SPBS_JOBID', batch_id)
                elif '-e' in items:
                    stdErr = items[-1].replace('$PBS_JOBID', batch_id)
        return stdOut, stdErr

    def get_max_bf_resources(self, workspec):
        resources = self.get_bf_resources(workspec, blocking=True)
        if resources:
            resources = self.adjust_resources(resources)
            if resources:
                return resources[0]
        return None
