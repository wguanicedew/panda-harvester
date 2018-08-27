try:
    import subprocess32 as subprocess
except:
    import subprocess
import uuid

from pandaharvester.harvestercore import core_utils
from .base_stager import BaseStager

# logger
baseLogger = core_utils.setup_logger('rucio_stager_hpc')


# plugin for stage-out with Rucio on an HPC site that must copy output elsewhere
class RucioStagerHPC(BaseStager):
    # constructor
    def __init__(self, **kwarg):
        BaseStager.__init__(self, **kwarg)
        if not hasattr(self, 'scopeForTmp'):
            self.scopeForTmp = 'panda'
        if not hasattr(self, 'pathConvention'):
            self.pathConvention = None
        if not hasattr(self, 'objstoreID'):
            self.objstoreID = None

    # check status
    def check_status(self, jobspec):
        # make logger
        tmpLog = self.make_logger(baseLogger, 'PandaID={0}'.format(jobspec.PandaID),
                                  method_name='check_status')
        tmpLog.debug('start')
        return (True,'')

    # trigger stage out
    def trigger_stage_out(self, jobspec):
        # make logger
        tmpLog = self.make_logger(baseLogger, 'PandaID={0}'.format(jobspec.PandaID),
                                  method_name='trigger_stage_out')
        tmpLog.debug('start')
        # loop over all files
        lifetime = 7*24*60*60
        allChecked = True
        ErrMsg = 'These files failed to upload : '
        zip_datasetName = 'harvester_stage_out.{0}'.format(str(uuid.uuid4()))
        fileAttrs = jobspec.get_output_file_attributes()
        for fileSpec in jobspec.outFiles:
            # fileSpec.fileAttributes['transferID'] = None #synchronius transfer
            # skip already done
            tmpLog.debug(' file: %s status: %s' % (fileSpec.lfn,fileSpec.status))                                                                                                                                             
            if fileSpec.status in ['finished', 'failed']:
                continue

            fileSpec.pathConvention = self.pathConvention
            fileSpec.objstoreID = self.objstoreID
            # set destination RSE
            if fileSpec.fileType in ['es_output', 'zip_output', 'output']:
                dstRSE = self.dstRSE_Out
            elif fileSpec.fileType == 'log':
                dstRSE = self.dstRSE_Log
            else:
                errMsg = 'unsupported file type {0}'.format(fileSpec.fileType)
                tmpLog.error(errMsg)
                return (False, errMsg)
            # skip if destination is None
            if dstRSE is None:
                continue
            
            # get/set scope and dataset name
            if fileSpec.fileType != 'zip_output':
                scope = fileAttrs[fileSpec.lfn]['scope']
                datasetName = fileAttrs[fileSpec.lfn]['dataset']
            else:
                # use panda scope for zipped files
                scope = self.scopeForTmp
                datasetName = zip_datasetName


            # for now mimic behaviour and code of pilot v2 rucio copy tool (rucio download) change when needed

            executable = ['/usr/bin/env',
                          'rucio', '-v', 'upload']
            executable += [ '--no-register' ]
            #executable += [ '--lifetime',('%d' %lifetime)]
            executable += [ '--rse',dstRSE]
            executable += [ '--scope',scope]
            #if fileSpec.fileAttributes is not None and 'guid' in fileSpec.fileAttributes:
            #    executable += [ '--guid',fileSpec.fileAttributes['guid']]
            #executable += [('%s:%s' %(scope,datasetName))]
            executable += [('%s' %fileSpec.path)]

            #print executable 

            tmpLog.debug('rucio upload command: {0} '.format(executable))
            tmpLog.debug('rucio upload command (for human): %s ' % ' '.join(executable))

            process = subprocess.Popen(executable,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT)

            stdout,stderr = process.communicate()
            
            if process.returncode == 0:
               fileSpec.status = 'finished'
               tmpLog.debug(stdout)
            else:
               # check what failed
               file_exists = False
               rucio_sessions_limit_error = False
               for line in stdout.split('\n'):
                  if 'File name in specified scope already exists' in line:
                     file_exists = True
                     break
                  elif 'exceeded simultaneous SESSIONS_PER_USER limit' in line:
                     rucio_sessions_limit_error = True
               if file_exists:
                  tmpLog.debug('file exists, marking transfer as finished')
                  fileSpec.status = 'finished'
               elif rucio_sessions_limit_error:
                  # do nothing
                  tmpLog.warning('rucio returned error, will retry: stdout: %s' % stdout)
                  # do not change fileSpec.status and Harvester will retry if this function returns False
                  allChecked = False
                  continue
               else:
                  fileSpec.status = 'failed'
                  tmpLog.error('rucio upload failed with stdout: %s' % stdout)
                  ErrMsg += '%s failed with rucio error stdout="%s"' % (fileSpec.lfn,stdout)
                  allChecked = False

            # force update
            fileSpec.force_update('status')

            tmpLog.debug('file: %s status: %s' % (fileSpec.lfn,fileSpec.status))                                      
            
        # return
        tmpLog.debug('done')
        if allChecked:
            return True, ''
        else:
            return False, ErrMsg


    # zip output files
    def zip_output(self, jobspec):
        # make logger
        tmpLog = self.make_logger(baseLogger, 'PandaID={0}'.format(jobspec.PandaID),
                                  method_name='zip_output')
        return self.simple_zip_output(jobspec, tmpLog)
