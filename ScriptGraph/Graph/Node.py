import ScriptGraph.BaseGraph.Graph as BaseGraph
import ScriptGraph.BaseGraph.Node as BaseNode
import os.path
import shutil

class Node( BaseNode.Node ):
    readyStatus      = "READY"
    blockedStatus    = "BLOCKED"
    completeStatus   = "COMPLETE"
    failedStatus       = "FAILED"
    runningStatus    = "RUNNING"
    def __init__( self, name = "" ):
        BaseNode.Node.__init__( self, name )
        self.status = ""
        self.workDir = ""
        self.fileListCache = []
        self.fileMapCache  = []

    def setWorkDir( self, dir ):
        if self.workDir:
            return
        self.workDir = dir
    
    def copyInputFiles( self ):
        if getattr( self, "inputFiles", [] ):
            for file in self.inputFiles:
                print "Copying file %s" % file
                shutil.copyfile( file, os.path.join( self.workDir, os.path.basename( file ) ) )

    def makeWorkDir( self ):
        return
        if not os.path.exists(self.workDir):
            os.makedirs( self.workDir )

    def getWorkDir( self ):
        return self.workDir

#   def getFiles( self ):
#       # returns the files we have
#       retval = []
#       for (key, currEdge) in self.parentEdges.items():
#           if ( self.currEdge.isReady() ):
#               retval.extend( self.currEdge.getOutputFiles() )
#       return retval
#   
    def isReady( self ):
        # is this node ready to send to children
        for currEdge in self.parentEdges.values():
            # recurse
            if ( not currEdge.isComplete() ):
                return False

        return True
        
    def getValueFromOnlyOutputFile( self ):
        if not getattr(self, "oneValueCache", [] ):
            print "Getvaluefromonlyinputfile: %s "  % self.getName()
            handle = open( self.getFiles()[0][0], 'r' )
            retval = handle.read()
            handle.close()
            retval = retval.strip()
            self.oneValueCache = retval
            return retval
        else:
            return self.oneValueCache


    def checkStatus( self ):
#       print "  Checking status of node: %s" % self.getName()
#       print "  ->parents %s" % self.parentEdges
        # call out to crab/whoever to see what the readiness is
        self.setComplete()
        for (key, currEdge) in self.parentEdges.items():
            # recurse
            currEdge.checkStatus()
            if currEdge.isFail():
                self.setFailed()
                return

            if not currEdge.isComplete() and not self.isFail():
                self.setBlocked()
                return

        for (key, currEdge) in self.childEdges.items():
            currEdge.checkStatus()
            if not currEdge.isComplete() and not self.isFail():
                self.setReady()
                return
        self.setComplete()
    
    def setReady( self ):
        self.status = self.readyStatus
    def setBlocked( self ):
        self.status = self.blockedStatus
    def setFailed( self ):
        self.status = self.failedStatus
    def setComplete( self ):
        self.status = self.completeStatus
    def isFail( self ):
        if not self.status:
            self.checkStatus()
        return ( self.status == self.failedStatus )

    def clearStatus( self ):
        #clear the rediness cache
        self.status = ""
    
    def getStatus( self ):
        return self.status

    def getFiles( self ):
        if self.fileListCache:
            return (self.fileListCache[:], self.fileMapCache)

        fileList = []
        fileMap  = {}
        
        for (key, currEdge) in self.parentEdges.items():
            if currEdge.isComplete():
                currOutput = currEdge.getOutputFiles()
                fileList.extend( currOutput )
                fileMap[ currEdge.getName() ] = currOutput

        self.fileListCache = fileList[:]
        self.fileMapCache  = fileMap
        return (fileList, fileMap)
    
    def getOnlyFile( self ):
        fileList, _ = self.getFiles()
        if len(fileList) != 1:
            raise RuntimeError, "Wanted one file, got %s" % len(fileList)
        return fileList[0]
