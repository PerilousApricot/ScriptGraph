#!/usr/bin/env python
import os, os.path, re, sys
import ScriptGraph.Graph.Graph as Graph
sys.path.append( os.path.dirname( os.path.abspath( __file__ )  ))
import genTree
from s8.MonitorInput import run_monitor_input_helper
from ScriptGraph.Helpers.Miter import Miter

g = Graph.Graph()
baseWorkDir = "/uscms_data/d2/meloam/dijetexercise"
g.setWorkDir( baseWorkDir )
treeGraph = genTree.getGraph()
exports   = genTree.getDatasets()

g.addGraph( treeGraph )

operating_points = ["TCHEM", "TCHEL",
                    "TCHPT",  "TCHPM",  "TCHPL",
                    "SSVT",   "SSVM",   "SSVL",
                    "SSVHET", "SSVHEM",
                    "SSVHPT" ]

# operating_points = operating_points[0:1]

jet_bins = [ ["40to60", "40..60"]
             ,["60to80", "60..80" ]
#             ,["80to140", "80..140"]
#             ,["140to", "140.."]
           ]


#
# Run s8_monitor_input over QCD without skipping events
#
djet10QCDMiter   = Miter()
for dataset in exports['qcd'].iterMany():
    for bin in jet_bins:
        for opoint in operating_points:
            step_postfix = "-%s-%s-%s-djet10mc" % (dataset[1]['dataset'],bin[0],opoint)
            currNode = run_monitor_input_helper( g,
                        jet_pt = bin[1],
                        tag = opoint,
                        fileKey = dataset[1]['dataset'],
                        trigger_name = "HLT_BTagMu_DiJet10U",
                        step_postfix = step_postfix,
                        muon_pt = "6..",
                        input_files = dataset[0] )
            dijet10QCDMiter.add( currNode, bin = bin[0], dataset=dataset[1]['dataset'], opoint=opoint )

for currmerge in dijet10QCDMiter.getGrouped( 'bin', 'dataset' ):
    step_postfix = "-%s-%s" % ( currmerge[1]['bin'], currmerge[1]['dataset'] )
    mergeNode = merge_with_root_qcd_helper( g, "root-qcd" + step_postfix, step_postfix,
                    inputNodes  = skiplessS8Monitor[ opoint ][ bin[0] ][ trigger[1] ],
                    triggerName = "hltjet10u",
                    lumiMiter   = exports['luminosityMiter']
                )

def getGraph():
    global g
    return g
#for opoint in operating_points:
#    for bin in jet_bins:
#        for sample in qcd_datasets:
#            for trigger in trigger_list:
#                step_postfix = "-%s-%s-%s-%s-noskip" % (sample[1],trigger[1],bin[0],opoint)
#                if not opoint in skiplessS8Monitor:
#                    skiplessS8Monitor[ opoint ] = {}
#                if not bin[0] in skiplessS8Monitor[ opoint ]:
#                    skiplessS8Monitor[ opoint ][ bin[0] ] = {}
#                if not trigger[1] in skiplessS8Monitor[ opoint ][ bin[0] ]:
#                    skiplessS8Monitor[ opoint ][ bin[0] ][ trigger[1] ] = []
#
#                currNode = run_monitor_input_helper( g,
#                            jet_pt = bin[1],
#                            tag = opoint,
#                            fileKey = sample[1],
#                            trigger_name = trigger[4],
#                            step_postfix = step_postfix,
#                            muon_pt = "6..",
#                            input_files = qcdTreeMiter.getOneValue( dataset =  sample[1] ),
#                            simulate_trigger = triggers_to_simulate[ trigger[1] ])
#                skiplessS8Monitor[ opoint ][ bin[0] ][ trigger[1] ].append( currNode )
#                singleMonitorMiter.add( currNode,dataset = sample[1],
#                                                trigger = trigger[1],
#                                                bin     = bin[0],
#                                                opoint  = opoint,
#                                                njetpt  = True,
#                                                njeteta = True,
#                                                privert = True,
#                                                type    = "skipless" )
# 
#
#                skiplessQCDMiter.add( currNode, dataset = sample[1],
#                                                trigger = trigger[1],
#                                                bin     = bin[0],
#                                                opoint  = opoint )
# 
#
#
#
