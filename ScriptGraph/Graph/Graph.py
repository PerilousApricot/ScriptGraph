#!/usr/bin/env python

import ScriptGraph.BaseGraph.Graph as BaseGraph
import unittest


from ScriptGraph.Graph.CondorScriptEdge import CondorScriptEdge
from ScriptGraph.Graph.NullEdge import NullEdge
import ScriptGraph.Graph.Node as Node
import ScriptGraph.Graph.Edge as Edge
import ScriptGraph.Graph.TestEdge as TestEdge
import ScriptGraph.Graph.CrabEdge as CrabEdge
import os.path
import os
import tempfile
import shutil
import sys
import commands
import re


class Graph( BaseGraph.Graph ):
    
    def __init__( self ):
        BaseGraph.Graph.__init__( self )
        self.workingDir = '.'
    
    def setWorkDir( self, dir ):
        """ sets the root of the workdir for this graph, this can be overrided though """
        self.workingDir = dir
        for node in self.nodes.values():
            node.setWorkDir( os.path.join( self.workingDir, "nodes", node.getName() ) )
        for edge in self.edges.values():
            edge.setWorkDir( os.path.join( self.workingDir, "edges", edge.getName() ) )
 
    def getWorkDir( self ):
        return self.workingDir

    def makeWorkDirs( self ):
        if not os.path.exists( self.workingDir ):
            os.makedirs( self.workingDir )
        for node in self.nodes.values():
            node.makeWorkDir()
        for edge in self.edges.values():
            edge.makeWorkDir()

    def clearStatus( self ):
        for node in self.nodes.values():
            node.clearStatus()
        for edge in self.edges.values():
            edge.clearStatus()

    def checkStatus( self ):
        count = 0
        for edge in self.edges.values():
            edge.checkStatus()
            count += 1
            if (count % 500) == 0:
                print "Checked %s/%s edges" % ( count, len(self.edges) )
        for node in self.nodes.values():
            node.checkStatus()   
    def getReadyNodes( self ):
        retval = []
        for node in self.nodes.values():
            if node.isReady():
                retval.append( node )
        return retval
    
    def getNextEdges( self ):
        readyNodes = self.getReadyNodes()
        edgeList = []
        for node in readyNodes:
            edgeList.extend( node.getChildren() )
        nextEdgeList = []
        for edge in edgeList:
            if edge.canRun() and not issubclass( edge.__class__, NullEdge):
                nextEdgeList.append( edge )
        return nextEdgeList

    maxProcesses = 16
    
    def pushGraph( self ):
        print "Pushing graph submissions, current edges"
        nextEdgeList = self.getNextEdges()
        nodeFileCache = {}
        nodeFileMapCache = {}
        currProcesses = 0
        currPids = {}
        masterPid = os.getpid()
        
        totalCondorJobs   = 0
        condorJobsToGroup = 0
        maxCondorJobs     = 3000
        if totalCondorJobs == 0:
            print "Calling out to condor_q"
            output = commands.getoutput("condor_q  `whoami`")
            matchObj = re.search("(\d+) jobs; (\d+) idle, (\d+) running, (\d+) held",output)
            if matchObj:
                totalCondorJobs = int(matchObj.group(1))

        condorGroups = {}
        for edge in nextEdgeList:
            edgeParent = edge.getParent().getName()
            if not nodeFileCache.get( edgeParent ):
                    nodeFileCache[ edgeParent ], nodeFileMapCache[ edgeParent ] =\
                        self.getNodeFiles( edgeParent )

            if currProcesses > self.maxProcesses:
                print "Waiting on child to return"
                while True:
                    onepid, _ = os.wait()
                    if onepid in currPids:
                        currProcesses -= 1
                        del currPids[ onepid ]
                        break
            
            if issubclass( edge.__class__, CondorScriptEdge ):
                
                if totalCondorJobs > maxCondorJobs:
                    print "Too many Condor jobs running, continuing.."
                    continue

                fileKey = edge.getFileKey()
                if fileKey:
                    if not fileKey in condorGroups:
                        condorGroups[ fileKey ] = []
                    condorGroups[ fileKey ].append( edge )

                    if len( condorGroups[ fileKey ] ) > condorJobsToGroup:
                        print "Executing batch of %s condor jobs together (shared input files)" % \
                                        len( condorGroups[ fileKey ] )

                        totalCondorJobs += 1
                        edge.executeMany( condorGroups[ fileKey ], nodeFileCache, nodeFileMapCache )
                        condorGroups[ fileKey ] = []
                        continue
                    else:
                        continue


            onePid = os.fork()
            if onePid == 0:
                edgeParent = edge.getParent().getName()
                print "  processing %s from %s to %s" % (edge.getName(), edge.getParent().getName(), edge.getChild().getName())
                edge.execute( nodeFileCache[ edgeParent ], nodeFileMapCache[ edgeParent ] )
                sys.stdout.flush()
                sys.stderr.flush()
                os._exit(0)
            else:
                currProcesses += 1
                currPids[ onePid ] = 1

            if os.getpid() != masterPid:
                print "Shouldn't be here!"
                os._exit(0)

        # looked at all the edges, drain the spare condor bits
        for fileKey in condorGroups:
            if totalCondorJobs < maxCondorJobs:
                if condorGroups[ fileKey ]:
                    print "Draining condor filekey %s" % fileKey
                    edge = condorGroups[ fileKey ][0]
                    edge.executeMany( condorGroups[ fileKey ], nodeFileCache, nodeFileMapCache )
                    totalCondorJobs += 1
        
    def getInputs( self ):
        """ Returns input nodes """
        retval = []
        for node in self.nodes.values():
            if not node.getParents():
                retval.append( node )
        return retval
    
    def getOutputs( self ):
        """ returns output nodes """
        retval = []
        for node in self.nodes.values():
            if not node.getChildren():
                retval.append( node )
        return retval

    def getNodeFiles( self, nodeName ):
        return self.nodes[ nodeName ].getFiles()
    
    def addGraph( self, o ):
        for node in o.getNodes().values():
            self.addNode( node )
        
        for edge in o.getEdges().values():
            self.addEdge( edge.getParent(), edge.getChild(), edge )

    def addNode( self, node ):
        BaseGraph.Graph.addNode( self, node )
        node.setWorkDir( os.path.join( 
                                self.workingDir,
                                "nodes",
                                node.getName() 
                             ) 
                       )
        node.makeWorkDir()
    
    def addEdge( self, parent, child, edge ):
        if not parent.getName() in self.nodes:
            raise RuntimeError, "Node '%s' hasn't been added to the graph yet" % parent.getName()
    
        if not child.getName() in self.nodes:
            raise RuntimeError, "Node '%s' hasn't been added to the graph yet" % child.getName()
        
        if edge.getName() in self.edges:
            raise RuntimeError, "Edge %s already exists" % edge.getName()
        
        BaseGraph.Graph.addEdge( self, parent, child, edge )
        edge.setWorkDir( os.path.join( self.workingDir, "edges", edge.getName() ) )
        edge.makeWorkDir()
#       for node in self.nodes.values():
#           node.copyInputFiles()
#       for edge in self.edges.values():
#           edge.copyInputFiles()
    
    def dumpInfo( self ):
        nodelist = self.nodes.keys()
        nodelist.sort()
        for nodekey in nodelist:
            nodeStatus = self.nodes[nodekey].getStatus()
            statusString = ""
            if nodeStatus:
                statusString = " [status=%s]" % nodeStatus
            print "Node %s%s" % (self.nodes[nodekey].getName(), statusString)

        edgelist = self.edges.keys()
        edgelist.sort()
        for edgekey in edgelist:
            edgeStatus = self.edges[edgekey].getStatus()
            statusString = ""
            if edgeStatus:
                statusString = " [status=%s]" % edgeStatus
            print "Edge %s links %s to %s%s" % (self.edges[edgekey].getName(),
                                                self.edges[edgekey].getParent().getName(),
                                                self.edges[edgekey].getChild().getName(),
                                                statusString )
    def dumpDot( self ):
        print "digraph finite_state_machine {"
        print 'size="8,5"'
        print 'node [shape = circle];'
        print "node [shape = doublecircle]; nullInput"
        edgelist = self.edges.values()
        for edge in edgelist:
            print '%s -> %s [ label = "%s" ];' % ( edge.getParent().getName().replace('-','_'), 
                                                    edge.getChild().getName().replace('-','_'),
                                                    edge.getName().replace('-','_') )
        print "}"
            
            


            

    def dumpInfo2( self ):
        nodekeys =  self.nodes.keys()
        nodekeys.sort()
        for nodekey in nodekeys:
            node = self.nodes[ nodekey ]
            nodeStatus   = node.status
            statusString = "" 
            if nodeStatus:
                statusString = " [status=%s]" % nodeStatus
                
            print "Node \"%s\":" % node.getName()
            print "  Working Directory: %s%s" % (node.getWorkDir(), statusString)
            children = node.getChildren()
            if children:
                print "  Child Edges:"
                for child in node.getChildren():
                    
                    edgeStatus = child.getStatus()
                    statusString = ""
                    if edgeStatus:
                        statusString = " [status=%s]" % edgeStatus
                    print "    %s => %s => %s%s" % (node.getName(),child.getName(), child.getChild().getName(), statusString)
            parents  = node.getParents()
            if parents:
                print "  Parent Edges:"
                for parent in node.getParents():
                    edgeStatus = parent.statusCache
                    statusString = ""
                    if edgeStatus:
                        statusString = " [status=%s]" % edgeStatus
        
                    print "    %s => %s => %s%s" % (parent.getParent().getName(), parent.getName(), node.getName(), statusString)


    
class TestGraph( unittest.TestCase ):
    def testMakeWorkDirs( self ):
        self.makeReadyGraph()
        self.assertTrue( os.path.exists( self.workdir ) )
        self.assertTrue( os.path.exists( os.path.join( self.workdir, "nodeA" ) ) )
        self.assertTrue( os.path.exists( os.path.join(  self.workdir, "nodeB" ) ) )
        self.assertTrue( os.path.exists( os.path.join(  self.workdir, "nodeC" ) ) )
        self.assertTrue( os.path.exists( os.path.join(  self.workdir, "nodeD" ) ) )
        self.assertTrue( os.path.exists( os.path.join(  self.workdir, "nodeA", "readyEdgeAB" ) ) )
        self.assertTrue( os.path.exists( os.path.join(  self.workdir, "nodeA", "readyEdgeAC" ) ) )
        self.assertTrue( os.path.exists( os.path.join(  self.workdir, "nodeC", "readyEdgeCD" ) ) )
        self.assertTrue( os.path.exists( os.path.join(  self.workdir, "nodeB", "readyEdgeBD" ) ) )


    def testGetNodeFiles1( self ):
        self.makeReadyGraph()
        self.assertEqual( len( self.myGraph.getNodeFiles(  "nodeA" )[0] ), 0 )
        self.assertEqual( len( self.myGraph.getNodeFiles(  "nodeB" )[0] ), 3 )
        self.assertEqual( len( self.myGraph.getNodeFiles(  "nodeC" )[0] ), 3 )
        self.assertEqual( len( self.myGraph.getNodeFiles(  "nodeD" )[0] ), 6 )
        print self.myGraph.getNodeFiles( "nodeD" )
    def testGetInputOutput1( self ):
        self.makeUnreadyGraph()
        self.assertEqual( len(self.myGraph.getInputs()), 1 )
        self.assertEqual( len(self.myGraph.getOutputs()), 1)
    
    def testGetInputOutput1( self ):
        self.makeReadyGraph()
        self.assertEqual( len(self.myGraph.getInputs()), 1 )
        self.assertEqual( len(self.myGraph.getOutputs()), 1)

    def setUp( self ):

        self.myGraph = Graph()
        self.workdir = tempfile.mkdtemp()
        self.myGraph.setWorkDir( self.workdir )
        self.nodeA   = Node.Node()
        self.nodeA.setName( "nodeA" )
        self.nodeB   = Node.Node()
        self.nodeB.setName( "nodeB" )
        self.nodeC   = Node.Node()
        self.nodeC.setName( "nodeC" )
        self.nodeD   = Node.Node()
        self.nodeD.setName( "nodeD" )

        self.edgeABReady  = TestEdge.AlwaysReadyEdge()
        self.edgeABReady.setName( "readyEdgeAB" )
        self.edgeABNotReady = TestEdge.NeverReadyEdge()
        self.edgeABNotReady.setName( "notReadyEdgeAB" )
        self.edgeACReady  = TestEdge.AlwaysReadyEdge()
        self.edgeACReady.setName( "readyEdgeAC" )
        self.edgeBDReady  = TestEdge.AlwaysReadyEdge()
        self.edgeBDReady.setName( "readyEdgeBD" )
        self.edgeCDReady  = TestEdge.AlwaysReadyEdge()
        self.edgeCDReady.setName( "readyEdgeCD" )

    def tearDown( self ):
        if getattr(self,"noDeleteTree", None):
            print "Not deleting the tree %s " % self.workdir
            return
        shutil.rmtree( self.workdir )

    def makeReadyGraph( self ):
        self.myGraph.addNode( self.nodeA )
        self.myGraph.addNode( self.nodeB )
        self.myGraph.addNode( self.nodeC )
        self.myGraph.addNode( self.nodeD )
        self.myGraph.addEdge( self.nodeA, self.nodeB, self.edgeABReady )
        self.myGraph.addEdge( self.nodeA, self.nodeC, self.edgeACReady )
        self.myGraph.addEdge( self.nodeB, self.nodeD, self.edgeBDReady )
        self.myGraph.addEdge( self.nodeC, self.nodeD, self.edgeCDReady )
    
    def makeUnreadyGraph( self ):
        self.myGraph.addNode( self.nodeA )
        self.myGraph.addNode( self.nodeB )
        self.myGraph.addNode( self.nodeC )
        self.myGraph.addNode( self.nodeD )
        self.myGraph.addEdge( self.nodeA, self.nodeB, self.edgeABNotReady )
        self.myGraph.addEdge( self.nodeA, self.nodeC, self.edgeACReady )
        self.myGraph.addEdge( self.nodeB, self.nodeD, self.edgeBDReady )
        self.myGraph.addEdge( self.nodeC, self.nodeD, self.edgeCDReady )
    
    def testPush( self ):
        self.makeUnreadyGraph()
        self.assertEqual( len( self.myGraph.getNextEdges() ), 1 )
        self.myGraph.pushGraph()

    def testReady( self ):
        self.makeReadyGraph()
        readyList = self.myGraph.getReadyNodes()
        self.assertEqual( len(readyList), 4 )
    
    def testNotReady( self ):
        self.makeUnreadyGraph()
        readyList = self.myGraph.getReadyNodes()
        self.assertEqual( len(readyList), 2 )
    
    def atestCrabSubmit( self ):
        self.noDeleteTree = True
        self.edgeABNotReady = CrabEdge.CrabEdge( "/uscms/home/meloam/s8/scriptGraph/ScriptGraph/Graph/crab.cfg", "/uscms_data/d2/meloam/RUN2010A/cmssw_cfg.py", [], [] )
        self.edgeABNotReady.setName("crabEdge")
        self.edgeABNotReady.addInputFile("input.txt")
        self.makeUnreadyGraph()
        print "CRAB CRAB CRAB"
        self.myGraph.pushGraph()
    

if __name__ == '__main__':
    unittest.main()
