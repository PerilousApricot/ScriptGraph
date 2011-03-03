#!/usr/bin/env python
import re
import getopt
from optparse import OptionParser
import sys
import imp
from ScriptGraph.Graph.NullEdge import NullEdge as NullEdge
def main():
#    usage = "usage: %prog [options] arg"
#   parser = OptionParser(usage)
#   parser.add_option("-c", "--cfg", type="string",
#                       dest="cfg")
    
                            
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hc:v", ["help", "cfg=", "list", "execute=", "describe=","push"])
    except getopt.GetoptError, err:
        # print help information and exit:
        print str(err) # will print something like "option -a not recognized"
        usage()
        sys.exit(2)
    output = None
    verbose = False
    cfg = "s8Workflow.py"
    for o, a in opts:
        if o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-c", "--cfg"):
            cfg = a
    confobj = imp.load_source( "myconfig", cfg )
    g = confobj.getGraph()
    for o, a in opts:
        if o == "--list":
            listGraph( g )
        if o == "--execute":
            executeEdge( g, a )
        if o == "--status":
            getStatus( g, a )
        if o == "--describe":
            describeEdge( g, a )
        if o == "--push":
            pushGraph(g)

def usage():
    #enter usage here
    print "scriptGraph -"
    print "Common arguments:"
    print "  --cfg=              : configuration with graph"
    print "  --list              : gets current processing status of graph"
    print "  --execute edgeName  : executes the given edge"
    print "  --status edgeName   : returns the status of a single edge"

def prettyPrintEdge( edge ):
    print " " + edge.getName() + " " + edge.getStatus()

def prettyPrintEdgeList( edges ):
    for edge in edges:
        prettyPrintEdge( edge )

def getStatus( g, name ):
    edge = g.getEdge( name )
    edge.checkStatus()

def globEdges( g, name ):
    name.replace( '*', '.*' )
    output = []
    for edge in g.getEdges():
        if re.match( name, edge ):
            output.append( edge )
    return output

def describeEdge( g, a ):
    a = g.getEdge( a )
    a.checkStatus()
    for edge in a.getParent().getParents():
        edge.checkStatus()
    for edge in a.getChild().getChildren():
        edge.checkStatus()

    a.getParent().isReady()

    print "Edge name %s type %s.%s" % (a.getName(), a.__module__, a.__class__)
    prettyPrintEdge( a )
    if a.getParent().getParents():
        print "Edge parents"
        for edge in a.getParent().getParents():
            prettyPrintEdge( edge )
    if a.getChild().getChildren():
        print "Edge children"
        for edge in a.getChild().getChildren():
            prettyPrintEdge( edge )


    if not a.getParent().isReady():
        print "Edge is blocked, parent name is %s" % a.getParent().getName()
        

def executeEdge( g, name ):
    edges = globEdges( g, name )
    print "got these back %s" % edges
    for edge in edges:
        executeOneEdge( g, edge )

def executeOneEdge( g, name ):
    edge = g.getEdge( name )
    parentNode = edge.getParent()
    edge.checkStatus()
    parentNode.checkStatus()
    filelist, filemap = parentNode.getFiles()
    print "Executing %s" % name
    edge.execute( filelist, filemap )
    print "Complete, checking status"
    edge.clearStatus()
    edge.checkStatus()
    print "Status is: %s [%s]" % ( edge.getName(), edge.getStatus() )

def pushGraph( g ):
    edgelist = g.getNextEdges()
    print "Edges ready to be pushed: "
    prettyPrintEdgeList( edgelist )
    if edgelist:
        print "Pushing ...."
        g.pushGraph()

def listGraph( g ):
    print "Loading status, (may take a long time)"
    g.checkStatus()
    runningEdges    = []
    blockedEdges    = []
    failEdges       = []
    pendingEdges    = []
    completeEdges   = []
    keylist = g.getEdges().keys()
    keylist.sort()
    for edgekey in keylist:
        edge = g.getEdges()[ edgekey ]
        if isinstance( edge, NullEdge ):
            continue
        if edge.isRunning():
            runningEdges.append( edge )
        if edge.isFail():
            failEdges.append( edge )
        if edge.isComplete():
            completeEdges.append( edge )
        
        if not edge.getParent().isReady() and edge.isIncomplete():
            blockedEdges.append( edge )
        elif edge.getParent().isReady() and edge.isIncomplete():
            pendingEdges.append( edge )
    
    if runningEdges:
        print "Running Edges====================="
        prettyPrintEdgeList( runningEdges )
    if blockedEdges:
        print "Blocked Edges====================="
        prettyPrintEdgeList( blockedEdges )
    if failEdges:
        print "Fail Edges========================"
        prettyPrintEdgeList( failEdges )
    if completeEdges:
        print "Complete Edges===================="
        prettyPrintEdgeList( completeEdges )
    if pendingEdges:
        print "Pending Edges====================="
        prettyPrintEdgeList( pendingEdges )








if __name__ == "__main__":
    main()


