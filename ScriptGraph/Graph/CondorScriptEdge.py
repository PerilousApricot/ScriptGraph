#!/usr/bin/env python

import ScriptGraph.Graph.Edge as Edge
import os.path
import os
import commands
import types
import re
import tarfile
import shutil
from ScriptGraph.Helpers.LateBind import LateBind

class CondorScriptEdge( Edge.Edge ):
    def __init__( self, command, output,
                        noEmptyFiles = False, 
                        name = "",
                        addFileNamesToCommandLine=False,
#                        inputSharing = None,
                        fileKey = None,
                        filePattern = None,
                        preludeLines = None):
        if not preludeLines:
            preludeLines = []

#        self.inputSharing = inputSharing
        self.filePattern  = filePattern
        self.fileKey = fileKey

        self.preludeLines = preludeLines
        self.command = command
        self.output  = output
        self.noEmptyFiles = noEmptyFiles
        self.addFileNames = addFileNamesToCommandLine
        Edge.Edge.__init__( self, name )
    
    def getFileKey( self ):
        return self.fileKey

    def fileExistsInWorkDir( self, name ):
        return os.path.exists( os.path.join( self.workDir, name ) )

    def checkStatusImpl( self ):
        self.clearOutputFiles()
        absOutputPath = os.path.join( self.workDir, self.output )


        if ( self.fileExistsInWorkDir( 'REDIRECT' ) and (not self.fileExistsInWorkDir( 'REDIRECT-COMPLETE' ) ) ):
            handle = open( os.path.join( self.workDir, 'REDIRECT' ), "r" )
            tarpath = handle.read()
            handle.close()
            if os.path.exists( tarpath ):
                tarhandle = tarfile.open( tarpath,
                                            "r" )
                tarhandle.extractall( self.workDir )
                if not self.fileExistsInWorkDir( 'COMPLETE' ):
                    handle = open( os.path.join( self.workDir, "COMPLETE" ),"w+" )
                    handle.write( "done" )
                    handle.close()
                if not self.fileExistsInWorkDir( 'REDIRECT-COMPLETE' ):
                    print "adding redirect"
                    handle = open( os.path.join( self.workDir, "REDIRECT-COMPLETE" ),"w+" )
                    handle.write( "done" )
                    handle.close()



        if ( self.fileExistsInWorkDir( 'COMPLETE' ) ):
            if ( self.fileExistsInWorkDir( 'RUNNING' ) ):
                os.unlink( os.path.join( self.workDir, 'RUNNING' ) )

            if ( self.fileExistsInWorkDir( self.output ) ):
                if ( self.noEmptyFiles and ( os.path.getsize( absOutputPath ))):
                    # we got the file and it wasn't empty
                    self.addOutputFile( absOutputPath )
                    self.setComplete()
                    return
                elif ( self.noEmptyFiles ):
                    # we got the file and it was empty
                    print ("Edge %s was set to not have zero-length " +\
                          "files but the output was zero length") %\
                          self.getName()
                    self.setFailed()
                    return              
                elif ( not self.noEmptyFiles ):
                    # we don't care if the file's empty or not
                    self.addOutputFile( targetPath )
                    self.setComplete()
                    return
                else:
                    raise RuntimeError, "Panic. Shouldn't be here"
            else:
                print ("Edge %s is marked as complete, but no output file "+\
                      "is here") % self.getName()
                self.setFailed()
                return
        elif ( self.fileExistsInWorkDir( 'RUNNING' ) ):
            if ( self.fileExistsInWorkDir( 'job-0.tar' ) ): 
                print "We have a jar"
                shutil.rmtree( self.workDir )
            self.setRunning()
            return
        elif ( self.fileExistsInWorkDir( 'FAILED' ) ):
            self.setFailed()
            return
        else:
            self.setIncomplete()
            return

    def makeJdl( self, executableString, transfer_files ):
        # make executable script
        executable = os.path.join( self.workDir,
                                    "SCRIPTGRAPH-EXECUTABLE.sh" )
        handle = open( executable, 'w' )
        handle.write(executableString)
        handle.close()

        # handle staging buddies
            

        # this JDL assumes the WN will also 
        jdlString = """Universe  = vanilla
#environment = CONDOR_ID=$(Cluster).$(Process)
#x509userproxy = /tmp/x509up_u43807
stream_output = false
stream_error  = false
notification  = never
should_transfer_files   = YES
when_to_transfer_output = ON_EXIT
copy_to_spool           = false
Executable = %(executable)s
+SGTaskID = "meloam_is_awesome"
%(transfer_files)s
output  = $(Cluster).$(Process).stdout.txt
error   = $(Cluster).$(Process).stderr.txt
log     = $(Cluster).$(Process).log.txt
queue 1
""" % { 'executable' : executable, 'transfer_files' : transfer_files }
        jdlPath = os.path.join( self.workDir, "SCRIPTGRAPH-JDL.txt" )
        handle = open( jdlPath, 'w' )
        handle.write( jdlString )
        handle.close()
        return jdlPath
    
    def generateExecutableString( self, executableString, counter ):
        prelines = "\n".join( self.preludeLines )
        execString = """(
SCRIPTGRAPH_ROOT_PATH=`pwd`
SG_FILES_OLD=`ls -1`

touch COMPLETE
tar -cvf job-%(counter)s.tar COMPLETE

SG_FILES_OLD=`ls -1`
touch RUNNING
rm COMPLETE
%(prelude)s
%(executable)s
touch COMPLETE
rm RUNNING
SG_FILES_NEW=`ls -1`

for DIR_FILE in ${SG_FILES_NEW//:/ }
do

    for SVN_FILE in ${SG_FILES_OLD//::/ }
    do
        if [ "${DIR_FILE}" = "job-%(counter)s.tar" ]
        then
            echo "Don't want to readd the tarball"
            continue 2
        fi

        if [ "${DIR_FILE}" = "${SVN_FILE}" ]
        then
            echo "Appears to be an input file: ${DIR_FILE}"
            continue 2
        fi

    done

    echo "Found a non-input-file, adding ${DIR_FILE}"
    tar -f job-%(counter)s.tar --append ${DIR_FILE}
    rm -rf ${DIR_FILE}
done


)

""" % { 'prelude' : prelines,
        'executable' : executableString,
        'counter' : counter }
        return execString

    def executeImpl( self, inputFiles, inputMap ):
        return self.executeMany( [ self ], inputFiles, inputMap )

    def getCommandString( self, inputFiles ):
        commandReplaced = None
        if isinstance( self.command, type([]) ):
            commandReplaced = []
            for onearg in self.command:
                commandReplaced.append( str( self.lateBind( onearg ) ) )
            if self.addFileNames:
                commandReplaced.extend( inputFiles )
            commandReplaced = " ".join( commandReplaced )

        elif isinstance( self.command, LateBind ):
            commandReplaced = self.lateBind( self.command )
            if self.addFileNames:
                commandReplaced = commandReplaced + ' ' + ' '.join( inputFiles )
        else:
            commandReplaced = self.command
            if self.addFileNames:
                commandReplaced = commandReplaced + ' ' + ' '.join( inputFiles )
    
        return commandReplaced

    def executeMany( self, edgeList, inputFiles, inputMap ):
        # needed?
#        if self.isRunning():
#            print "Already running (possibly due to job clustering)"
#            return

        oldcwd = os.getcwd()
        os.chdir( self.workDir )

        #
        # Generate the file list (we're staging them to the WN)
        #
#        print "file list %s" % inputFiles
#        filesToStage = inputFiles[ self.getParent().getName() ]
#        if self.filePattern:
#            chosenFiles = []
#            for file in filesToStage:
#                if re.search( self.filePattern, file ):
#                    chosenFiles.append( file )
#            filesToStage = chosenFiles
#
        filesToStageString = "#transfer_input_files = " # + ",".join(filesToStage)
        wrapperString = "#!/bin/bash\necho 'Beginning wrapper'\n\n"
        counter = 0
        for edge in edgeList:
            commandReplaced = edge.getCommandString( inputFiles )
            print "Executing: %s" % commandReplaced
            wrapperString += edge.generateExecutableString( commandReplaced, counter )
            handle = open( os.path.join( edge.workDir, "REDIRECT" ), 'w+' )
            handle.write( os.path.join(self.workDir, "job-%s.tar" % counter ) )
            handle.close()
            handle = open( os.path.join( edge.workDir, "RUNNING" ), 'w+' )
            handle.write( "running" )
            handle.close()

            counter += 1

        jdlPath = self.makeJdl( wrapperString, filesToStageString )
        print "JDL is at: %s" % jdlPath
        status, output = commands.getstatusoutput( "condor_submit %s" \
                                % jdlPath )
        if not status:
            handle = open( os.path.join( self.workDir, "RUNNING" ), 'w' )
            handle.write( "%s" % output )
            handle.close()

        os.chdir( oldcwd )
        print output
