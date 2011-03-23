from ScriptGraph.Graph.Node import Node
from ScriptGraph.Graph.NullEdge import NullEdge
from ScriptGraph.Graph.CondorScriptEdge import CondorScriptEdge
from ScriptGraph.Helpers.Miter import Miter
from ScriptGraph.Helpers.BindFileList import BindFileList
from ScriptGraph.Graph.LocalScriptEdge import LocalScriptEdge

def run_monitor_input_helper(   g,
                                step_postfix,   
                                input_files = None, #can be a node from event generation
                                muon_pt = None,
                                jet_pt  = None,
                                trigger_name = None,
                                log = None,
                                output = None,
                                tag = None,
                                fileKey = None,
                                skip_events = False,
                                event_count = False,
                                additional_dependencies = False,
                                data=False,
                                reweight_trigger = False,
                                simulate_trigger = False,
                                use_condor = True):
    collectNode = Node( name = "collect-s8_monitor_input" + step_postfix )
    currNode    = Node( name = "s8_monitor_input" + step_postfix )
    g.addNode( collectNode )
    g.addNode( currNode )
    
    if input_files and isinstance( input_files, Node ):
        g.addEdge( input_files, collectNode, NullEdge() )
    elif input_files and isinstance( input_files, type([]) ):
        for node in input_files:
            g.addEdge( node, collectNode, NullEdge() )

    if not additional_dependencies:
        additional_dependencies = []
    for dep in additional_dependencies:
        g.addEdge( dep, collectNode, NullEdge() )
    
    commandLine = ["s8_monitor_input"]
    if log:
        commandLine.extend(["-d", log])
    if muon_pt:
        commandLine.extend(["--muon-pt=%s"% muon_pt])
    if trigger_name:
        commandLine.extend(["--trigger=%s"% trigger_name])
    if output:
        commandLine.extend(["-o", output])
    else:
        output = "output.root"
    if tag:
        commandLine.extend(["--tag", tag])
    if event_count:
        commandLine.extend(["-e", event_count])
    if skip_events:        
        commandLine.extend(["-s", skip_events])
    if jet_pt:
        commandLine.extend(["--jet-pt", jet_pt])
    if data:
        commandLine.extend(["--data", data])
    if input_files and \
            (isinstance( input_files, Node ) or\
             isinstance( input_files, type([]) ) ):
        if use_condor:
            commandLine.extend(["-i", BindFileList( name="input.txt", filePattern="s8_tree", relative=False)])
        else:
            commandLine.extend(["-i", BindFileList( name="input.txt", filePattern="s8_tree", relative=False)])

    elif input_files:
        raise RuntimeError, "no input files? %s" % input_files

    if reweight_trigger:
        commandLine.extend(["--reweight-trigger", reweight_trigger])
    if simulate_trigger:
        commandLine.extend(["--simulate-trigger" ])
    if not fileKey:
        raise RuntimeError, "Need a file key"

    currEdge = None
    if use_condor:
        currEdge = CondorScriptEdge( \
            filePattern = "s8_tree",
            fileKey = fileKey,
            name = "run_s8_monitor_input" + step_postfix,
            command = commandLine,
            preludeLines = [ "OLDCWD=`pwd`", "cd /uscms/home/meloam/","source sets8.sh","cd $OLDCWD" ],
            output = output,
            noEmptyFiles = True)
    else:
        currEdge = LocalScriptEdge( \
            name = "run_s8_monitor_input" + step_postfix,
            command = commandLine,
            output = output,
            noEmptyFiles = True)

    g.addEdge( collectNode, currNode, currEdge )
    return currNode
    
