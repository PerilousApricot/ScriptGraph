#!/usr/bin/env python

import ScriptGraph.Graph.Edge as Edge
import os.path
import os
import commands
import types
import re
from ScriptGraph.Helpers.LateBind import LateBind

class CondorScriptEdge( Edge.Edge ):
    def __init__( self, command, output,
                        noEmptyFiles = False, 
                        name = "",
                        addFileNamesToCommandLine=False,
                        inputSharing = None,
                        fileKey = None,
                        filePattern = None,
                        preludeLines = None):
        if not preludeLines:
            preludeLines = []

        if not filePattern:
            filePattern = ".*"

        self.inputSharing = inputSharing
        self.filePattern  = filePattern
        self.fileKey = fileKey

        self.preludeLines = preludeLines
        self.command = command
        self.output  = output
        self.noEmptyFiles = noEmptyFiles
        self.addFileNames = addFileNamesToCommandLine
        Edge.Edge.__init__( self, name )
    
    def fileExistsInWorkDir( self, name ):
        return os.path.exists( os.path.join( self.workDir, name ) )

    def checkStatusImpl( self ):
        self.clearOutputFiles()
        absOutputPath = os.path.join( self.workDir, self.output )

        if ( self.fileExistsInWorkDir( 'COMPLETE' ) ):
            if ( self.fileExistsInWorkDir( self.output ) ):
                if ( self.noEmptyFiles and ( os.path.getsize( absOutputPath ))):
                    # we got the file and it wasn't empty
                    self.addOutputFile( absOutputPath )
                    self.setComplete()
                    return
                elif ( self.noEmptyFiles ):
                    # we got the file and it was empty
                    print "Edge %s was set to not have zero-length " +\
                          "files but the output was zero length" %\
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
output  = stdout.txt
error   = stderr.txt
log     = log.txt
queue 1
""" % { 'executable' : executable, 'transfer_files' : transfer_files }
        jdlPath = os.path.join( self.workDir, "SCRIPTGRAPH-JDL.txt" )
        handle = open( jdlPath, 'w' )
        handle.write( jdlString )
        handle.close()
        return jdlPath
    
    def generateExecutableString( self, executableString ):
        prelines = "\n".join( self.preludeLines )
        execString = """#!/bin/bash
touch RUNNING
rm COMPLETE
%(prelude)s
%(executable)s
touch COMPLETE
rm RUNNING
""" % { 'prelude' : prelines,
        'executable' : executableString }
        return execString
    
    def executeImpl( self, inputFiles, inputMap ):
        if self.isRunning():
            print "Already running (possibly due to job clustering)"
            return

        filesToStage = inputFiles

        if self.filePattern:
            filesToStage = []
            for file in inputFiles:
                if re.search( self.filePattern, file ):
                    filesToStage.append( file )

        filesToStageString = "transfer_input_files = " + ",".join(filesToStage)

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
        
        print "Executing: %s" % commandReplaced
        wrapperString = self.generateExecutableString( commandReplaced )
        print "Wrapper is: %s" % wrapperString
        jdlPath = self.makeJdl( wrapperString, filesToStageString )
        print "JDL is at: %s" % jdlPath
        status, output = commands.getstatusoutput( "condor_submit %s" \
                                % jdlPath )
        if not status:
            handle = open( os.path.join( self.workDir, "RUNNING" ), 'w' )
            handle.write( "%s" % output )
            handle.close()

        print output
