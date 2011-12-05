from ScriptGraph.Graph.Node import Node
from ScriptGraph.Graph.NullEdge import NullEdge
from ScriptGraph.Helpers.Miter import Miter
from ScriptGraph.Helpers.BindFileList import BindFileList
from ScriptGraph.Graph.LocalScriptEdge import LocalScriptEdge
from ScriptGraph.Helpers.BindFunction import BindFunction
import os.path

def getFileNameStub( args ):
    print "looking at %s" % args['node'].getName()
    try:
        return args['node'].getOnlyFile()
    except:
        print "failed at %s" % args['node'].getName()
        raise


def makeTableHelper(   g,
                                step_postfix,   
                                reference = None,
                                mc = None,
                                syst1 = None,
                                syst2 = None,
                                syst3 = None,
                                systname = None,
                                outlatex = None
                                ):
    
    
    collectNode = Node( name = "collect-maketable" + step_postfix )
    currNode    = Node( name = "maketable" + step_postfix )
    g.addNode( collectNode )
    g.addNode( currNode )
    for dep in [reference,mc,syst1,syst2]:
        if dep:
            g.addEdge( dep.getChild(), collectNode, NullEdge() )
    
    command = ["mkTable.py"]
    if reference:
        command.extend(["--reference", BindFunction( func = getFileNameStub,
                                                     args = { 'node' : reference.getChild() } ) ] )
    if mc:
        command.extend(["--mc", BindFunction( func = getFileNameStub,
                                                     args = { 'node' : mc.getChild() } ) ] )
    if syst1:
        command.extend(["--syst1", BindFunction( func = getFileNameStub,
                                                     args = { 'node' : syst1.getChild() } ) ] )
    if syst2:
        command.extend(["--syst2", BindFunction( func = getFileNameStub,
                                                     args = { 'node' : syst2.getChild() } ) ] )
    if syst3:
        command.extend(["--syst3", BindFunction( func = getFileNameStub,
                                                     args = { 'node' : syst3.getChild() } ) ] )
    if systname:
        command.extend(["--systname", systname])

    if outlatex:
        command.extend(["--outlatex", outlatex])

    currEdge = LocalScriptEdge( \
        name = "mktable" + step_postfix,
        command = command,
        output = systname,
        noEmptyFiles = True)

    g.addEdge( collectNode, currNode, currEdge )
    return currNode
    
