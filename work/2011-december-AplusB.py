#!/usr/bin/env python
import os, os.path, re, sys, commands
import ScriptGraph.Graph.Graph as Graph
from ScriptGraph.Graph.Node import Node
from ScriptGraph.Graph.LocalScriptEdge import LocalScriptEdge
from ScriptGraph.Helpers.BindFunction import BindFunction
from ScriptGraph.Helpers.BindSubstitutes import BindSubstitutes
from ScriptGraph.Helpers.BindPreviousOutput import BindPreviousOutput
from ScriptGraph.Graph.NullEdge import NullEdge
from ScriptGraph.Graph.LambdaEdge import LambdaEdge

sys.path.append( os.path.dirname( os.path.abspath( __file__ )  ))

# Set the data selection
data_era = "AplusB" # or "A" or "B"
pvweight_base = "/uscms/home/meloam/s8/CMSSW_4_2_4/src/RecoBTag/PerformanceMeasurements/test/CrabFiles/weights/Weight3Dfinebin"
pv_weight = "%s%s.root" % ( pvweight_base, data_era )
if data_era == "AplusB":
    import genTree2011SummerAplusB2 as genTree
elif data_era == "A":
    import genTree2011SummerA2 as genTree
elif data_era == "B":
    import genTree2011SummerB2 as genTree



trigger_map = genTree.trigger_map

from s8.MonitorInput2 import run_monitor_input_helper
from s8.RootQCD import merge_with_root_qcd_helper
from s8.Hadd import hadd_helper
from s8.MakeTable import makeTableHelper
from ScriptGraph.Helpers.Miter import Miter

g = Graph.Graph()
singleMonitorMiter = Miter()
baseWorkDir = "/uscms_data/d3/meloam/2011-december-%s" % data_era

g.setWorkDir( baseWorkDir )
treeGraph = genTree.getGraph()
exports   = genTree.getDatasets()

g.addGraph( treeGraph )

simple_mode    = False
do_comparisons = False
do_plots       = False

binName = "14bin"
binID   = 2 # see run_s8.C for this

operating_points = ["SSVHEM","TCHEM", "TCHEL", "TCHET",
                    "TCHPT",  "TCHPM",  "TCHPL",
                    "SSVT",   "SSVM",   "SSVL",
                    "SSVHET",
                    "SSVHPT" ]
operating_points = ["SSVHEM", "SSVHPT", "TCHEL", "TCHEM", "TCHPM", "TCHPT","JPL"]
operating_points = ["TCHEM","TCHEL",
                    "TCHPT","TCHPM",
                    "SSVHEM",
                    "SSVHPT","JPL","JPM","JPT",
                    "JPBL","JPBM","JPBT",
                    "CSVL","CSVM","CSVT"]
operating_points = ["SSVHEM", "TCHEM"]
if simple_mode:
    operating_points = ["SSVHEM","TCHEM"]
    pass



jet_bins_mc = [
         [ "20to"  , "20..",  "15", "210"]
]

jet_bins_data = [
        [ "20to50" , "20..50","15","55" ],
        [ "50to80" , "50..80","45","85" ],
        [ "80to120", "80..120","75","125" ],
        [ "120to" , "120..","115","210" ],
]
dataTriggerMiter = Miter()

dataTriggerMiter.add( trigger_map['hltdijet20'], bin="20..50" )

dataTriggerMiter.add( trigger_map['hltdijet20'], bin="50..80" )
dataTriggerMiter.add( trigger_map['hltdijet40'], bin="50..80" )

dataTriggerMiter.add( trigger_map['hltdijet20'], bin="80..120" )
dataTriggerMiter.add( trigger_map['hltdijet40'], bin="80..120" )
dataTriggerMiter.add( trigger_map['hltdijet70'], bin="80..120" )

dataTriggerMiter.add( trigger_map['hltdijet110'], bin="120.." )

singleDataMonitorNode = Node( "singleDataMonitor" )
g.addNode( singleDataMonitorNode )
singleDataMonitor = Miter()
def addSingleMonitor( node, **kwargs ):
    global singlaDataMonitorNode, singleDataMonitor, g
    singleDataMonitor.add( node, **kwargs )
    dummy = NullEdge()
    g.addEdge( node, singleDataMonitorNode, dummy )

haddedDataMiter = Miter()
dataForPlots = Miter()

systematicDataList = [
    [ "nominal", [] ],
    [ "awaytchel", ["--away-tag=TCHEL"] ],
    [ "awaytchpm", ["--away-tag=TCHPM"] ],
    [ "mu7",  [] ],
    [ "mu10", [] ],
	[ "gsplit", [] ],
    [ "lowpv", ["--primary-vertices=0..4"] ],
    [ "medpv", ["--primary-vertices=4..6"] ],
    [ "lowpv-ptrel", ["--primary-vertices=0..5"] ],
    [ "highpv-ptrel", ["--primary-vertices=5.."] ],
    [ "highpv",["--primary-vertices=6.."]  ],
    [ "loweta",["--jet-eta=0..1.2"]],
    [ "higheta",["--jet-eta=1.2..2.4"]],
    [ "ttbar", []],
    [ "drop5B", []],
    [ "drop10B", []],
    [ "drop20B", []],
    [ "scalebm05", []],
    [ "scalebm10", []],
    [ "scalebm20", []],
    [ "scalebp05", []],
    [ "scalebp10", []],
    [ "scalebp20", []],
    [ "scaleclm05", []],
    [ "scaleclm10", []],
    [ "scaleclm20", []],
    [ "scaleclp05", []],
    [ "scaleclp10", []],
    [ "scaleclp20", []],
    [ "scalenm05", []],
    [ "scalenm10", []],
    [ "scalenm20", []],
    [ "scalenp05", []],
    [ "scalenp10", []],
    [ "scalenp20", []],
    [ "scalepm05", []],
    [ "scalepm10", []],
    [ "scalepm20", []],
    [ "scalepp05", []],
    [ "scalepp10", []],
    [ "scalepp20", []],


]

systematicsMCUsingNominalData =  ["drop5B","drop10B", "drop20B", "ttbar",
     "scalebm05",
     "scalebm10", 
     "scalebm20", 
     "scalebp05", 
     "scalebp10", 
     "scalebp20", 
     "scaleclm05",
     "scaleclm10",
     "scaleclm20",
     "scaleclp05",
     "scaleclp10",
     "scaleclp20",
     "scalenm05",
     "scalenm10", 
     "scalenm20", 
     "scalenp05", 
     "scalenp10", 
     "scalenp20",
     "scalepm05",
     "scalepm10", 
     "scalepm20", 
     "scalepp05", 
     "scalepp10", 
     "scalepp20",


     ]

if simple_mode:
    systematicDataList = [ [ "nominal", [] ] ]
#    systematicsMCUsingNominalData = []
#        [ "awaytchel", ["--away-tag=TCHEL"] ],
#        [ "awaytchpm", ["--away-tag=TCHPM"] ],
#        [ "mu7",  [] ],
#        [ "mu10", [] ],
#        [ "gsplit", [] ],
#        [ "lowpv-ptrel", ["--primary-vertices=0..5"] ],
#        [ "highpv-ptrel", ["--primary-vertices=5.."] ],
#        [ "loweta",["--jet-eta=0..1.2"]],
#        [ "higheta",["--jet-eta=1.2..2.4"]],
#        [ "ttbar", []] 

systematicsMCUsingNominalData = ["drop5B", "drop10B", "drop20B","scalebm05",
     "scalebm10", 
     "scalebm20", 
     "scalebp05", 
     "scalebp10", 
     "scalebp20", 
     "scaleclm05",
     "scaleclm10",
     "scaleclm20",
     "scaleclp05",
     "scaleclp10",
     "scaleclp20",
     "scalenm05",
     "scalenm10", 
     "scalenm20", 
     "scalenp05", 
     "scalenp10", 
     "scalenp20",
     "scalepm05",
     "scalepm10", 
     "scalepm20", 
     "scalepp05", 
     "scalepp10", 
     "scalepp20",]

if simple_mode:
    pass
    #systematicsMCUsingNominalData = []
    
systematicsDataMiter = Miter()
for systematic in systematicDataList:

    # Some systematics just borrows data from nominal
    if systematic[0] in systematicsMCUsingNominalData:
        continue

    for opoint in operating_points:
        for bin in jet_bins_data:
            for tmap in dataTriggerMiter.iterGrouped( 'bin' ):
                if tmap.vals[0][1]['bin'] != bin[1]:
                    continue
                
                sample      = None
                triggerList = []
                for onetmap in tmap.iterMany():
                    sample = onetmap[0][5]
                    trigger = onetmap[0]
                    triggerList.extend( [ trigger[0] ] )
    
                step_postfix = "-%s-%s-%s-%s-%s" % (systematic[0],sample,trigger[1],bin[0],opoint)
                muon_cut = "6.."
                if systematic[0] == "mu7":
                    muon_cut = "7.."
                if systematic[0] == "mu10":
                    muon_cut = "10.."
                                    
                monitor_node =\
                        run_monitor_input_helper( g,
                            jet_pt = bin[1],
                            tag = opoint,
                            fileKey = sample,
                            trigger_name = triggerList,
                            step_postfix = step_postfix,
                            muon_pt = muon_cut,
                            data = "1",
                            syst = systematic[0],
                            additional_arguments = systematic[1],
                            input_files = exports['data'].getValues( trigger=trigger[1] ))
        
                addSingleMonitor( monitor_node, dataset = sample,
                                                     trigger = trigger[1],
                                                     bin     = bin[0],
                                                     opoint  = opoint,
                                                     syst = systematic[0],
                                                     type = "data" )

                systematicsDataMiter.add( monitor_node, dataset = sample,
                                                     trigger = trigger[1],
                                                     bin     = bin[0],
                                                     opoint  = opoint,
                                                     syst    = systematic[0] )

                # ttbar doesn't require recomputing the data
                if ( systematic[0] == 'nominal' ):
                    for borrow_syst in systematicsMCUsingNominalData:
                        systematicsDataMiter.add( monitor_node, dataset = sample,
                                                         trigger = trigger[1],
                                                         bin     = bin[0],
                                                         opoint  = opoint,
                                                         syst    = borrow_syst )
                        

#
# Merge triggers
#
dataForWeightsSyst = Miter()
for systematic in systematicDataList:
    for opoint in operating_points:
        for bin in jet_bins_data:
#            if opoint == operating_points[0] and systematic[0] == systematicDataList[0][0]:
#                dataForWeightsSyst.add( dataForWeights[ bin[0] ],
#                                        bin = bin[0],
#                                        syst = 'gsplit' )
            currNode = hadd_helper( g, "-%s-%s-%s-data" % (systematic[0],bin[0],opoint), 
                                            systematicsDataMiter.getValues( bin=bin[0],
                                            opoint = opoint,
                                            syst = systematic[0]) )
            addSingleMonitor( currNode,\
                                                    bin     = bin[0],
                                                    opoint  = opoint,
                                                    syst    = systematic[0],
                                                    type    = "data_binned" )

#
# Merge Bins
#
haddedDataFinalSystMiter = Miter()
for systematic in systematicDataList:   
    for opoint in operating_points:
#    for opoint in haddedDataMiter.iterGrouped( 'opoint' ):
#        if (opoint.vals[0][1]['opoint'] == operating_points[0]) and \
#           (systematic[0] == systematicDataList[0][0]):
#            haddedDataFinalSystMiter.add( haddedDataFinalMiter.getOneValue ( opoint = opoint.vals[0][1]['opoint'] ),
#
#                                                    opoint = opoint.vals[0][1]['opoint'],
#                                                    syst = 'gsplit' )
        currNode = hadd_helper( g, "-%s-%s-data-final" % (systematic[0],
                                                        opoint,), 
                                    systematicsDataMiter.getValues( opoint = opoint,
                                                                syst=systematic[0]) )
        addSingleMonitor( currNode,\
                                            opoint  = opoint,
                                            syst    = systematic[0],
                                            type    = "data_final" )

        haddedDataFinalSystMiter.add( currNode, opoint  = opoint,
                                                    syst    = systematic[0] )
        dataForPlots.add( currNode,                 opoint  = opoint,
                                                    syst    = systematic[0],
                                                    bin     = 'all' )
                                               
        if opoint == operating_points[0]:
            dataForWeightsSyst.add( currNode, syst=systematic[0] )



#################
###### At this point, we have all we need for data
#################
preweightMergeMiter = Miter()
mcForPlots = Miter()
                                           
#
# Generate QCD for systematics
#                                          
systematicsMCList = [
    [ "nominal", [] ],
    [ "awaytchel", ["--away-tag=TCHEL"] ],
    [ "awaytchpm", ["--away-tag=TCHPM"] ],
    [ "mu7",  [] ],
    [ "mu10", [] ],
    [ "gsplit", ["--g-split=add_bb"]],
    [ "lowpv", ["--primary-vertices=0..4"] ],
    [ "medpv", ["--primary-vertices=4..6"] ],
    [ "highpv",["--primary-vertices=6.."]  ],
    [ "lowpv-ptrel", ["--primary-vertices=0..5"] ],
    [ "highpv-ptrel", ["--primary-vertices=5.."] ],
    [ "loweta",["--jet-eta=0..1.2"]],
    [ "higheta",["--jet-eta=1.2..2.4"]],
    [ "ttbar",[]],
    [ "drop5B", ["--dropBFraction=0.05"]], 
    [ "drop10B", ["--dropBFraction=0.10"]], 
    [ "drop20B", ["--dropBFraction=0.20"]], 
    [ "scalebm05", []],
    [ "scalebm10", []],
    [ "scalebm20", []],
    [ "scalebp05", []],
    [ "scalebp10", []],
    [ "scalebp20", []],
    [ "scaleclm05", []],
    [ "scaleclm10", []],
    [ "scaleclm20", []],
    [ "scaleclp05", []],
    [ "scaleclp10", []],
    [ "scaleclp20", []],
    [ "scalenm05", []],
    [ "scalenm10", []],
    [ "scalenm20", []],
    [ "scalenp05", []],
    [ "scalenp10", []],
    [ "scalenp20", []],
    [ "scalepm05", []],
    [ "scalepm10", []],
    [ "scalepm20", []],
    [ "scalepp05", []],
    [ "scalepp10", []],
    [ "scalepp20", []],


]


systematicsUsingNominalMC =  ["scalebm05",
     "scalebm10", 
     "scalebm20", 
     "scalebp05", 
     "scalebp10", 
     "scalebp20", 
     "scaleclm05",
     "scaleclm10",
     "scaleclm20",
     "scaleclp05",
     "scaleclp10",
     "scaleclp20",
     "scalenm05",
     "scalenm10", 
     "scalenm20", 
     "scalenp05", 
     "scalenp10", 
     "scalenp20",
     "scalepm05",
     "scalepm10", 
     "scalepm20", 
     "scalepp05", 
     "scalepp10", 
     "scalepp20",
     "ttbar" ]

solversToSkip =  ["scalebm05",
     "scalebm10", 
     "scalebm20", 
     "scalebp05", 
     "scalebp10", 
     "scalebp20", 
     "scaleclm05",
     "scaleclm10",
     "scaleclm20",
     "scaleclp05",
     "scaleclp10",
     "scaleclp20",
     "scalenm05",
     "scalenm10", 
     "scalenm20", 
     "scalenp05", 
     "scalenp10", 
     "scalenp20",
     "scalepm05",
     "scalepm10", 
     "scalepm20", 
     "scalepp05", 
     "scalepp10", 
     "scalepp20",
]

if simple_mode:
    systematicsMCList = [ [ "nominal", [] ] ]
#            [ "awaytchel", ["--away-tag=TCHEL"] ],
#            [ "awaytchpm", ["--away-tag=TCHPM"] ],
#            [ "mu7",  [] ],
#            [ "mu10", [] ],
#            [ "gsplit", ["--g-split=add_bb"]],
#            [ "lowpv-ptrel", ["--primary-vertices=0..5"] ],
#            [ "highpv-ptrel", ["--primary-vertices=5.."] ],
#            [ "loweta",["--jet-eta=0..1.2"]],
#            [ "higheta",["--jet-eta=1.2..2.4"]],
#            [ "ttbar",[]],

#            ]


#
# Run s8_monitor_input over QCD, for the input to reweighting (so we don't care about tag
# or trigger, only the bin) SYST
#
preweightMiterSyst   = Miter()
for systematic in systematicsMCList:
    # Some systematics just borrows data from nominal
    if systematic[0] in systematicsUsingNominalMC:
        continue

    for dataset in exports['qcd'].iterMany():
        for bin in jet_bins_mc:
               
            step_postfix = "-%s-%s-%s-preweight" % (dataset[1]['dataset'],bin[0],systematic[0])
            currNode = run_monitor_input_helper( g,
                        jet_pt = bin[1],
    #                    tag = operating_,
                        fileKey = dataset[1]['dataset'],
    #                        trigger_name = "HLT_BTagMu_DiJet10U",
                        step_postfix = step_postfix,
                        muon_pt = "6..",
						additional_arguments = systematic[1],
                        syst = systematic[0],
                        input_files = dataset[0] )
            preweightMiterSyst.add( currNode, bin = bin[0], dataset=dataset[1]['dataset'], syst=systematic[0] )

#
# Merge the pthats SYST
#

mcForWeightsSyst = Miter()
preweightMergeMiterSyst = Miter()
for systematic in systematicsMCList:
    # Some systematics just borrows data from nominal
    if systematic[0] in systematicsUsingNominalMC:
        continue

    for currmerge in preweightMiterSyst.iterGrouped( 'bin', 'syst' ):
		if ( systematic[0] == currmerge.vals[0][1]['syst'] ):
			step_postfix = "-%s-%s-preweight" % ( currmerge.vals[0][1]['bin'], systematic[0] )
			mergeNode = merge_with_root_qcd_helper( g, "root-qcd" + step_postfix, step_postfix,
							inputNodes  = currmerge.getValues(), pu = True
						)
			preweightMergeMiterSyst.add( mergeNode,
								bin     = currmerge.vals[0][1]['bin'] )

			mcForWeightsSyst.add(mergeNode, bin=  currmerge.vals[0][1]['bin'] , syst= systematic[0] )

    if ( systematic[0] == 'nominal' ):
        for borrow_syst in systematicsUsingNominalMC:
            systematicsDataMiter.add( monitor_node, dataset = sample,
                                         trigger = trigger[1],
                                         bin     = bin[0],
                                         opoint  = opoint,
                                         syst    = borrow_syst )


#
# SYST Calculate the weights (on a bin-only basis) SYST
#
weightMiterSyst = Miter()

#print "data %s mc %s" % (dataForWeights, mcForWeights )
for systematic in systematicsMCList:
    # Some systematics just borrows data from nominal
    if systematic[0] in systematicsUsingNominalMC:
        continue


    step_key    = "-%s" % ( systematic[0] )
    step_postfix= step_key
    collectNode = Node( name = "collect-reweight" + step_postfix )
    weightNode  = Node( name = "weight" + step_postfix )
    reweighNode = Node( name = "reweighed" + step_postfix )
    print "what is systematic: %s" % systematic[0]
    dataNode    = dataForWeightsSyst.getOneValue( syst = systematic[0] )
    mcNode      = mcForWeightsSyst.getOneValue( syst = systematic[0] )
    # Usage: root_trigger_weights out.root data_monitor.root mc_monitor.root
    calcWeightEdge = LocalScriptEdge(
                    name = "calcweight" +  step_postfix,
                    command = ["root_trigger_weights",
                                    bin[1],
                                    #"-3..3",
                                    "weight.root",
                                    BindPreviousOutput( dataNode ),
                                    BindPreviousOutput( mcNode   ) ],
                    output = "weight.root",
                    noEmptyFiles = True )
    g.addNode( collectNode )
    g.addNode( weightNode )
    g.addEdge( dataNode, collectNode, NullEdge() )
    g.addEdge( mcNode, collectNode, NullEdge()   )
    g.addEdge( collectNode, weightNode, calcWeightEdge )

    weightMiterSyst.add( calcWeightEdge, syst=systematic[0] )
    if systematic[0] == 'nominal':
        for borrow_syst in systematicsUsingNominalMC:
            weightMiterSyst.add( calcWeightEdge, syst=borrow_syst )

#
# reweight s8_monito_input 
#

reweightSystMiter = Miter()
for systematic in systematicsMCList:
    # Some systematics just borrows data from nominal
    if systematic[0] in systematicsUsingNominalMC:
        continue

    for opoint in operating_points:
        for bin in jet_bins_mc:
            if systematic[0] == 'ttbar':
                datasetList = exports['ttbar'].iterMany()
            else:
                datasetList = exports['qcd'].iterMany()

            for dataset in datasetList:
                sample     = dataset[0]
                samplename = dataset[1]['dataset'] 
                step_postfix= "-%s-%s-%s-%s-%s-reweight" % ( samplename,systematic[0], trigger[1], bin[0], opoint )
                step_key    = "-%s-%s-%s-%s" % ( trigger[1], systematic[0],bin[0], opoint )
                # now, use the weights to run s8_monitor_input again
                deps = [
                         weightMiterSyst.getOneValue( syst = systematic[0] ).getChild(),
                         # we automatically include deps to input files
                         #qcdTreeNodes[ sample[1]
                       ]
    
                muon_cut = "6.."
                if systematic[0] == "mu7":
                    muon_cut = "7.."
                if systematic[0] == "mu10":
                    muon_cut = "10.."
                reweightNode = run_monitor_input_helper( g,
                            step_postfix,
                            fileKey = samplename,
                            jet_pt  = bin[1],
                            tag = opoint,
                            input_files = sample,
                            muon_pt = muon_cut,
                            syst = systematic[0],
                            additional_dependencies = deps,
                            additional_arguments = systematic[1],
                            reweight_trigger_pv = pv_weight,
                            reweight_trigger = BindPreviousOutput( weightMiterSyst.getOneValue( syst = systematic[0] ) ) )
                reweightSystMiter.add( reweightNode, 
                                        opoint = opoint,
                                        bin = bin[0], 
                                        dataset= samplename,
                                        syst = systematic[0] )

#
# Merge the pthats SYST
#

reweightMergePthatSystMiter = Miter()
for currmerge in reweightSystMiter.iterGrouped( 'bin', 'opoint', 'syst' ):
        
    step_postfix = "-%s-%s-%s-reweight" % ( currmerge.vals[0][1]['syst'],currmerge.vals[0][1]['bin'],currmerge.vals[0][1]['opoint'] )
    mergeNode = merge_with_root_qcd_helper( g, "root-qcd" + step_postfix, step_postfix,
                    inputNodes  = currmerge.getValues(), pu = True
                )
    reweightMergePthatSystMiter.add( mergeNode,
                        bin     = currmerge.vals[0][1]['bin'],
                        opoint  = currmerge.vals[0][1]['opoint'],
                        syst    = currmerge.vals[0][1]['syst'])

    if ( currmerge.vals[0][1]['syst'] == 'nominal' ):
        for borrow_syst in systematicsMCUsingNominalData:
            reweightMergePthatSystMiter.add( mergeNode,
                        bin     = currmerge.vals[0][1]['bin'],
                        opoint  = currmerge.vals[0][1]['opoint'],
                        syst    = borrow_syst )

    
                        

#
# Merge Bins SYST
#
haddedMCFinalSystMiter = Miter()
for opoint in reweightMergePthatSystMiter.iterGrouped( 'opoint', 'syst' ):
    currNode = hadd_helper( g, "-%s-%s-qcd-mergebin" % (opoint.vals[0][1]['opoint'],
                                                     opoint.vals[0][1]['syst']), 
                                reweightMergePthatSystMiter.getValues( opoint = opoint.vals[0][1]['opoint'],
                                                                       syst   = opoint.vals[0][1]['syst']) )
    haddedMCFinalSystMiter.add( currNode, opoint = opoint.vals[0][1]['opoint'],
                                          syst   = opoint.vals[0][1]['syst'])
    mcForPlots.add( currNode, opoint = opoint.vals[0][1]['opoint'],
                                          syst   = opoint.vals[0][1]['syst'],
                                          bin    = 'all')


allPages = Miter()
#webRoot  = "/scratch/meloam/btag/2011plots/"
#webroot   = "/afs/fnal.gov/files/home/room3/meloam/public_html/s8/2011plots"
#webRoot  = "/uscms_data/d2/meloam/input39x2/edges/plots/"
#httpRoot = "http://home.fnal.gov/~meloam/s8/39x/"
#print singleMonitorMiter.vals
def getFileNameStub( args ):
    print "looking at %s" % args['node'].getName()
    try:
        return args['node'].getOnlyFile()
    except:
        print "failed at %s" % args['node'].getName()
        raise


s8macro = "/uscms/home/meloam/s8/CMSSW_4_2_4/src/RecoBTag/PerformanceMeasurements/test/S8Solver/run_s8.C"
scaleFactors = Miter()

solversToSkip =  ["scalebm05",
     "scalebm10", 
     "scalebm20", 
     "scalebp05", 
     "scalebp10", 
     "scalebp20", 
     "scaleclm05",
     "scaleclm10",
     "scaleclm20",
     "scaleclp05",
     "scaleclp10",
     "scaleclp20",
     "scalenm05",
     "scalenm10", 
     "scalenm20", 
     "scalenp05", 
     "scalenp10", 
     "scalenp20",
     "scalepm05",
     "scalepm10", 
     "scalepm20", 
     "scalepp05", 
     "scalepp10", 
     "scalepp20",
]
closureSolver = Miter()
for (onedata, onemc) in haddedDataFinalSystMiter.zip( haddedMCFinalSystMiter ):
    opoint = onedata[1]['opoint']
    syst   = onedata[1]['syst']
    if syst in solversToSkip:
        continue
    collect = Node( name = "collect-s8solve-close-%s-%s" % (syst,opoint) )
    solution= Node( name = "s8volve-close-%s-%s" % (syst, opoint) )
    g.addNode( collect )
    g.addNode( solution )
    g.addEdge( onedata[0], collect, NullEdge() )
    g.addEdge( onemc[0]  , collect, NullEdge() )

    runs8edge = LocalScriptEdge(
                        name = "s8solver-closure-%s-%s-%s" % (syst,opoint,binName) , 
                        command = BindSubstitutes("root -b -q '%s(\"%s\",\"%s\")' &> output.txt" % \
                            ( s8macro, '%s','%s'),#, binID ),
                              [BindFunction( func = getFileNameStub,
                                             args = { 'node': onemc[0] } ),
                               BindFunction( func = getFileNameStub,
                                             args = { 'node': onemc[0] } )] ),
                        output="output.txt", 
                        noEmptyFiles=True)
    g.addEdge( collect, solution, runs8edge )
    scaleFactors.add( runs8edge, type = "closure", opoint=opoint )
    closureSolver.add( runs8edge, opoint = opoint, syst = syst )

def getFileNameForClosureStub( args ):
    print "looking at %s" % args['node'].getName()
    try:
        return os.path.join( os.path.dirname( args['node'].getOnlyFile() ), "s8.root" )
    except:
        print "failed at %s" % args['node'].getName()
        raise

normalSolver = Miter()
for (onedata, onemc) in haddedDataFinalSystMiter.zip( haddedMCFinalSystMiter ):
    opoint = onedata[1]['opoint']
    syst   = onedata[1]['syst']
    if syst in solversToSkip:
        continue
    collect = Node( name = "collect-s8solve-%s-%s" % (syst,opoint) )
    solution= Node( name = "s8volve-%s-%s" % (syst,opoint) )
    g.addNode( collect )
    g.addNode( solution )
    g.addEdge( onedata[0], collect, NullEdge() )
    g.addEdge( onemc[0]  , collect, NullEdge() )

    runs8edge = LocalScriptEdge(
                        name = "s8solver-%s-%s-%s" % (syst, opoint,binName) , 
                        command = BindSubstitutes("root -b -q '%s(\"%s\",\"%s\")' &> output.txt" % \
                            ( s8macro, '%s','%s'),#, binID ),
                              [BindFunction( func = getFileNameStub,
                                             args = { 'node': onedata[0] } ),
                               BindFunction( func = getFileNameStub,
                                             args = { 'node': onemc[0] } )] ),
                        output="output.txt", 
                        noEmptyFiles=True)
    g.addEdge( collect, solution, runs8edge )
    scaleFactors.add( runs8edge, type = "nominal" )
    normalSolver.add( runs8edge, opoint = opoint, syst = syst )

for (onedata, onemc) in haddedDataFinalSystMiter.zip( haddedMCFinalSystMiter ):
    opoint = onedata[1]['opoint']
    syst   = onedata[1]['syst']
    if syst == 'nominal':
        for ptstuff in [ ['ptrel12',2],['ptrel05',3] ]:
            syst  = ptstuff[0]
            extra = ptstuff[1]

            collect = Node( name = "collect-s8solve-%s-%s" % (syst,opoint) )
            solution= Node( name = "s8volve-%s-%s" % (syst,opoint) )
            g.addNode( collect )
            g.addNode( solution )
            g.addEdge( onedata[0], collect, NullEdge() )
            g.addEdge( onemc[0]  , collect, NullEdge() )

            runs8edge = LocalScriptEdge(
                                name = "s8solver-%s-%s-%s" % (syst, opoint, binName) , 
                                command = BindSubstitutes("root -b -q '%s(\"%s\",\"%s\",%s)' &> output.txt" % \
                                    ( s8macro, '%s','%s', extra),#,binID ),
                                      [BindFunction( func = getFileNameStub,
                                                     args = { 'node': onedata[0] } ),
                                       BindFunction( func = getFileNameStub,
                                                     args = { 'node': onemc[0] } )] ),
                                output="output.txt", 
                                noEmptyFiles=True)
            g.addEdge( collect, solution, runs8edge )
            normalSolver.add( runs8edge, syst = syst, opoint = opoint )

# raise/lower tags
raiseLower = [
    [ "scalebm05", 0.95,1.0 ],
    [ "scalebm10", 0.90,1.0 ],
    [ "scalebm20", 0.80,1 ],
    [ "scalebp05", 1.05,1 ],
    [ "scalebp10", 1.10,1 ],
    [ "scalebp20", 1.20,1 ],
    [ "scaleclm05",1.00,0.95 ],
    [ "scaleclm10",1.00,0.90 ],
    [ "scaleclm20",1.00,0.80 ],
    [ "scaleclp05",1.00,1.05 ],
    [ "scaleclp10",1.00,1.10 ],
    [ "scaleclp20",1.00,1.20 ],
    [ "scalepm05",1,1, 0.95,1.0 ],
    [ "scalepm10",1,1, 0.90,1.0 ],
    [ "scalepm20",1,1, 0.80,1 ],
    [ "scalepp05",1,1, 1.05,1 ],
    [ "scalepp10",1,1, 1.10,1 ],
    [ "scalepp20",1,1, 1.20,1 ],
    [ "scalenm05",1,1,1.00,0.95 ],
    [ "scalenm10",1,1,1.00,0.90 ],
    [ "scalenm20",1,1,1.00,0.80 ],
    [ "scalenp05",1,1,1.00,1.05 ],
    [ "scalenp10",1,1,1.00,1.10 ],
    [ "scalenp20",1,1,1.00,1.20 ],



]
for (onedata, onemc) in haddedDataFinalSystMiter.zip( haddedMCFinalSystMiter ):
    opoint = onedata[1]['opoint']
    syst   = onedata[1]['syst']
    if syst == 'nominal':
        for ptstuff in raiseLower:
            print ptstuff
            syst    = ptstuff[0]
            bscale  = ptstuff[1]
            clscale = ptstuff[2]
            collect = Node( name = "collect-s8solve-%s-%s" % (syst,opoint) )
            solution= Node( name = "s8volve-%s-%s" % (syst,opoint) )
            g.addNode( collect )
            g.addNode( solution )
            g.addEdge( onedata[0], collect, NullEdge() )
            g.addEdge( onemc[0]  , collect, NullEdge() )
        
            runs8edge = LocalScriptEdge(
                                name = "s8solver-%s-%s-%s" % (syst, opoint,binName) , 
                                command = BindSubstitutes("root -b -q '%s(\"%s\",\"%s\",%s,%.2g,%.2g)' &> output.txt" % \
                                    ( s8macro, '%s','%s',0,bscale,clscale),#,binID ),
                                      [BindFunction( func = getFileNameStub,
                                                     args = { 'node': onedata[0] } ),
                                       BindFunction( func = getFileNameStub,
                                                     args = { 'node': onemc[0] } )] ),
                                output="output.txt", 
                                noEmptyFiles=True)
            g.addEdge( collect, solution, runs8edge )
            normalSolver.add( runs8edge, syst = syst, opoint = opoint )



combinedMiter = Miter()
if do_comparisons:
    #python ../../scripts/mkTable.py --reference ~yumiceva/lpc1/S8_results_Jun3/edges/s8solver-closure-nominal-SSVHEM/output.txt --syst1 ~yumiceva/lpc1/S8_results_Jun3/edges/s8solver-gsplit-SSVHEM/output.txt --systname syst_gsplit_SSVHEM.txt --outlatex syst_gsplit_SSVHEM.tex
    for opoint in operating_points:
        combinedMiter.add(makeTableHelper( g, reference = closureSolver.getOneValue( syst = 'nominal', opoint = opoint ),
                            syst1     = closureSolver.getOneValue( syst = 'gsplit', opoint = opoint ),
                            systname  = "syst.txt",
                            outlatex  = "latex.txt",
                            step_postfix = "%s-%s-%s" % (opoint, 'gsplit',binName)),
                        opoint = opoint,
                        syst = 'gsplit')


    #python ../../scripts/mkTable.py --reference ~yumiceva/lpc1/S8_results_Jun3/edges/s8solver-nominal-SSVHEM/output.txt --syst1 ~yumiceva/lpc1/S8_results_Jun3/edges/s8solver-mu7-SSVHEM/output.txt --syst2 ~yumiceva/lpc1/S8_results_Jun3/edges/s8solver-mu10-SSVHEM/output.txt --systname syst_mupt_SSVHEM.txt
    for opoint in operating_points:
        combinedMiter.add(makeTableHelper( g, reference = normalSolver.getOneValue( syst = 'nominal', opoint = opoint ),
                            syst1     = normalSolver.getOneValue( syst = 'mu7', opoint = opoint ),
                            syst2     = normalSolver.getOneValue( syst = 'mu10', opoint = opoint ),
                            systname  = "syst.txt",
                            outlatex  = "latex.txt",
                            step_postfix = "%s-%s-%s" % (opoint, 'mu',binName)
                            ),
                        opoint = opoint,
                        syst = 'mu')


    #python ../../scripts/mkTable.py --reference ~yumiceva/lpc1/S8_results_Jun3/edges/s8solver-nominal-SSVHEM/output.txt --syst1 ~yumiceva/lpc1/S8_results_Jun3/edges/s8solver-mu7-SSVHEM/output.txt --syst2 ~yumiceva/lpc1/S8_results_Jun3/edges/s8solver--SSVHEM/output.txt --systname syst_awaytag_SSVHEM.txt --outlatex syst_awaytag_SSVHEM.tex
    for opoint in operating_points:
        combinedMiter.add(makeTableHelper( g, reference = normalSolver.getOneValue( syst = 'nominal', opoint = opoint ),
                            syst1     = normalSolver.getOneValue( syst = 'awaytchel', opoint = opoint ),
                            syst2     = normalSolver.getOneValue( syst = 'awaytchpm', opoint = opoint ),
                            systname  = "syst.txt",
                            outlatex  = "latex.txt",
                            step_postfix = "%s-%s-%s" % (opoint, 'awaytag',binName)),
                        opoint = opoint,
                        syst = 'awaytag')


    #python ../../scripts/mkTable.py --reference  ~yumiceva/lpc1/S8_results_Jun3/edges/s8solver-closure-nominal-SSVHEM/output.txt --mc ~yumiceva/lpc1/S8_results_Jun3/edges/s8solver-closure-nominal-SSVHEM/output.txt --systname syst_mc_SSVHEM.txt --outlatex syst_mc_SSHEM.tex
    for opoint in operating_points:
        combinedMiter.add(makeTableHelper( g, reference = closureSolver.getOneValue( syst = 'nominal', opoint = opoint ),
                            mc        = closureSolver.getOneValue( syst = 'nominal', opoint = opoint ),
                            systname  = "syst.txt",
                            outlatex  = "latex.txt",
                            step_postfix = "%s-%s-%s" % (opoint, 'closure', binName)),
                        opoint = opoint,
                        syst = 'closure')


    #python ../../scripts/mkTable.py --reference ~yumiceva/lpc1/S8_results_Jun3/edges/s8solver-nominal-SSVHEM/output.txt --syst1 /uscms_data/d2/pratima/System8/May26/CMSSW_4_1_5/src/RecoBTag/PerformanceMeasurements/test/S8Solver/ptrel/low/output.txt --syst2 /uscms_data/d2/pratima/System8/May26/CMSSW_4_1_5/src/RecoBTag/PerformanceMeasurements/test/S8Solver/ptrel/hi/output.txt --systname syst_ptrel_SSVHEM.txt --outlatex syst_ptrel_SSHEM.tex
    for opoint in operating_points:
        combinedMiter.add(makeTableHelper( g, reference = normalSolver.getOneValue( syst = 'nominal', opoint = opoint ),
                            syst1     = normalSolver.getOneValue( syst = 'ptrel05', opoint = opoint ),
                            syst2     = normalSolver.getOneValue( syst = 'ptrel12', opoint = opoint ),
                            systname  = "syst.txt",
                            outlatex  = "latex.txt",
                            step_postfix = "%s-%s-%s" % (opoint, 'ptrel',binName)),
                        opoint = opoint,
                        syst = 'ptrel')

#    for opoint in operating_points:
#        combinedMiter.add(makeTableHelper( g, reference = normalSolver.getOneValue( syst = 'nominal', opoint = opoint ),
#                            syst1     = normalSolver.getOneValue( syst = 'ttbar', opoint = opoint ),
#                            systname  = "syst.txt",
#                            outlatex  = "latex.txt",
#                            step_postfix = "%s-%s-%s" % (opoint, 'ttbar',binName)),
#                        opoint = opoint,
#                        syst = 'ttbar')

    for opoint in operating_points:
        combinedMiter.add(makeTableHelper( g, reference = normalSolver.getOneValue( syst = 'nominal', opoint = opoint ),
                            syst1     = normalSolver.getOneValue( syst = 'lowpv-ptrel', opoint = opoint ),
                            syst2     = normalSolver.getOneValue( syst = 'highpv-ptrel', opoint = opoint ),
                            systname  = "syst.txt",
                            outlatex  = "latex.txt",
                            step_postfix = "%s-%s-%s" % (opoint, 'pu-ptrel',binName)),
                        opoint = opoint,
                        syst = 'pu-ptrel')



    for opoint in operating_points:
        combinedMiter.add(makeTableHelper( g, reference = normalSolver.getOneValue( syst = 'nominal', opoint = opoint ),
                            syst1     = normalSolver.getOneValue( syst = 'lowpv', opoint = opoint ),
                            syst2     = normalSolver.getOneValue( syst = 'medpv', opoint = opoint ),
                            syst3     = normalSolver.getOneValue( syst = 'highpv', opoint = opoint ),
                            systname  = "syst.txt",
                            outlatex  = "latex.txt",
                            step_postfix = "%s-%s-%s" % (opoint, 'pu',binName)),
                        opoint = opoint,
                        syst = 'pu')

    for opoint in operating_points:
        combinedMiter.add(makeTableHelper( g, reference = normalSolver.getOneValue( syst = 'nominal', opoint = opoint ),
                            syst1     = normalSolver.getOneValue( syst = 'drop5B', opoint = opoint ),
                            syst2     = normalSolver.getOneValue( syst = 'drop10B', opoint = opoint ),
                            syst3     = normalSolver.getOneValue( syst = 'drop20B', opoint = opoint ),
                            systname  = "syst.txt",
                            outlatex  = "latex.txt",
                            step_postfix = "%s-%s-%s" % (opoint, 'dropB',binName)),
                        opoint = opoint,
                        syst = 'dropB')


def getTotalAddStub( args ):
    try:
        return "%s:%s" % (args['name'], args['node'].getOnlyFile())
    except:
        print "failed at %s" % args['node'].getName()
        raise


def getTableBind( name, input ):
    return BindFunction( func = getTotalAddStub,
                   args = { 'node' : input,
                            'name' : name } )

# make the sums
# /uscms/home/meloam/s8/CMSSW_4_1_5/src/RecoBTag/PerformanceMeasurements/scripts/calcTotal.py
if do_comparisons:
    for opoint in operating_points:
        step_postfix = "%s-pasbin2" % opoint
        command = [ "/uscms/home/meloam/s8/CMSSW_4_1_5/src/RecoBTag/PerformanceMeasurements/scripts/calcTotal.py",
                    "output.txt",
                    getTableBind("away", combinedMiter.getOneValue( opoint = opoint, syst = "awaytag" ) ),
                    getTableBind("mu", combinedMiter.getOneValue( opoint = opoint, syst = "mu" ) ),
                    getTableBind("ptrel", combinedMiter.getOneValue( opoint = opoint, syst = "ptrel" ) ),
                    getTableBind("gsplit", combinedMiter.getOneValue( opoint = opoint, syst = "gsplit" ) ),
#                    getTableBind("ttbar", combinedMiter.getOneValue( opoint = opoint, syst = "ttbar" ) ),
                    getTableBind("closure", combinedMiter.getOneValue( opoint = opoint, syst = "closure" ) ),
                    getTableBind("pu", combinedMiter.getOneValue( opoint = opoint, syst = "pu" ) ) ]

        currEdge = LocalScriptEdge( \
            name = "calcTotal" + step_postfix,
            command = command,
            output = "output.txt",
            noEmptyFiles = True)
        collectNode = Node( name="collect-calc-%s" % opoint )
        g.addNode( collectNode )
        for oneNode in combinedMiter.iterManyMatchingConditions( opoint = opoint ):
            g.addEdge( oneNode[0], collectNode, NullEdge() )
        calcOut     = Node( name="calctable-%s" % opoint )
        g.addNode( calcOut )
        g.addEdge( collectNode, calcOut, currEdge )
        


def getS8RootFile( args ):
    return args['node'].getWorkDir() + "/s8.root"

webRoot   = "/afs/fnal.gov/files/home/room3/meloam/public_html/s8/2011-october-AplusB/%s" % binName
#webRoot  = "/scratch/meloam/btag/2011-mayplot/"
#webRoot  = "/uscms_data/d2/meloam/input39x2/edges/plots/"
httpRoot = "http://home.fnal.gov/~meloam/s8/2011-october-AplusB/%s" % binName

#
# Generate single monitoring plots
#
singleMonitorImages = Miter()
singleMonitorTarget = Node( name = "singleMonitorTarget" )
singleMonitorAbs = os.path.join( webRoot, 'singleMonitor' )
singleMonitorRel  = 'singleMonitor'
# input is singleDataMonitorNode
g.addNode( singleMonitorTarget )

def getFileNameStub( args ):
    return args['node'].getOnlyFile()


def doSingleComparison( self , inputFiles, inputMap , inputMiter = None ):
    if inputMiter == None:
        raise "Need a list of input files"

    singleComparisonMacro = '/uscms/home/meloam/scratch/s8workflow/SingleComparison.C'

    singleMonitorPlotList = Miter()
    singlePlotLabelList = ['syst','opoint','bin','dataset','trigger','pthat','type']
    for onenode in inputMiter.iterMany():

        print "miter is %s" % onenode[1]
        monitorNames = [ [ "MonitorAnalyzer/n/njet_pt", "-njetpt.png", "Njet_{pt}" ],
                         [ "MonitorAnalyzer/n/njet_eta","-njeteta.png", "Njet_{eta}"],
                         [ "MonitorAnalyzer/generic/pvs","-pvs.png", "N_{pv}" ],
                         [ "MonitorAnalyzer/n/njet_pt", "-njetpt.pdf", "Njet_{pt}" ],
                         [ "MonitorAnalyzer/n/njet_eta","-njeteta.pdf", "Njet_{eta}"],
                         [ "MonitorAnalyzer/generic/pvs","-pvs.pdf", "N_{pv}" ] ]


        fileDesc = []
        for descriptor in singlePlotLabelList:
            if descriptor in onenode[1]:
                fileDesc.append( onenode[1][descriptor] )
        subMonitorAbs = [ singleMonitorAbs ]
        subMonitorAbs.extend( fileDesc[:-1] )
        subMonitorAbs = os.path.join( *subMonitorAbs )#, onenode[1]['opoint'], onenode[1]['bin'] )
        subMonitorRel = [ singleMonitorRel ]
        subMonitorRel.extend( fileDesc[:-1] )
        subMonitorRel = os.path.join( *subMonitorRel )#, onenode[1]['opoint'], onenode[1]['bin'] )

        for oneMonitor in monitorNames:
            #root -b -q 'SingleComparison.C("edges/run_s8_monitor_input-RUN2010A-hltjet10u-60to80-TCHEM-noskip/output.root","MonitorAnalyzer/n/njet_pt","test2.png","this is a header")'
            targetFileNameAbs = os.path.join( subMonitorAbs, "%s%s" % ( fileDesc[-1], oneMonitor[1] ) )
            if not os.path.exists( subMonitorAbs ):
                os.makedirs( subMonitorAbs )
            targetFileNameRel = os.path.join( subMonitorRel, "%s%s" % ( fileDesc[-1], oneMonitor[1] ) )
            print "making this %s" % targetFileNameRel
            command = "root -b -q '%s(\"%s\",\"%s\",\"%s\",\"%s\")'" % \
                                    ( singleComparisonMacro, getFileNameStub( { 'node' : onenode[0] } ), 
                                        oneMonitor[0], targetFileNameAbs, oneMonitor[2] )
            print "executing plot %s" % command
            output = commands.getoutput( command )
            print output
#        singleMonitorPlotList.add( targetFileNameRel, opoint = onenode[1]['opoint'], 
#                                                      bin    = onenode[1]['bin'],
#                                                      trigger= onenode[1]['trigger'] if 'trigger' in onenode[1] else "none" )
#
#

def singleStatus( self, **kwargs ):
    self.setIncomplete()

singleEdge = LambdaEdge( name = "plotSingle",
                         command = doSingleComparison,
                         inputMiter = singleDataMonitor,
                         status = singleStatus )
#g.addEdge( singleDataMonitorNode, singleMonitorTarget, singleEdge )


#
# DOUBLE PLOT COMPARISON
#
doubleComparisonMacro = '/uscms/home/meloam/scratch/s8workflow/DoubleComparison.C'
doubleMonitorPlotList = Miter()
doubleMonitorImages = Miter()
doubleMonitorTarget = Node( name = "doubleMonitorTarget" )
doubleMonitorAbs = os.path.join( webRoot, 'doubleMonitor' )
doubleMonitorRel  = 'doubleMonitor'
count = 0
g.addNode( doubleMonitorTarget )

# get together all the different plots we want to compare
allDoublePlots = []
print "vals from mcforplots" % mcForPlots.vals
print "vals from dataforplots" % dataForPlots.vals
for (onedata, onemc) in dataForPlots.zip( mcForPlots ):
    allDoublePlots.append( [ onedata, onemc, "Data", "Simulation", "complete", onedata[1]['bin']] )

 
for (onedata, onemc, dataLabel, mcLabel, plotLabel, plotBin) in allDoublePlots:
    print "zipping %s %s" % (onedata[1], onemc[1] )

    singlePlotLabelList = ['opoint','syst','bin']
    fileDesc = []
    for descriptor in singlePlotLabelList:
        if descriptor in onedata[1]:
            fileDesc.append( onedata[1][descriptor] )

    subMonitorAbs = [ doubleMonitorAbs ]
    subMonitorAbs.extend( fileDesc[:-1] )
    subMonitorAbs = os.path.join( *subMonitorAbs )#, onenode[1]['opoint'], onenode[1]['bin'] )
    subMonitorRel = [ doubleMonitorRel ]
    subMonitorRel.extend( fileDesc[:-1] )
    subMonitorRel = os.path.join( *subMonitorRel )#, onenode[1]['opoint'], onenode[1]['bin'] )
    print "got %s %s" % (subMonitorRel, subMonitorAbs)
    minbin = 20
    maxbin = 240
#    if 'bin' in onedata[1]:
#        for onebin in jet_bins:
#            if onebin[0] == onedata[1]['bin']:
#                minbin = onebin[2]
#                maxbin = onebin[3]
#
    monitorNames = [ [ "MonitorAnalyzer/n/njet_pt", "-njetpt", "Njet_{pt}" ],
                     [ "MonitorAnalyzer/n/njet_eta","-njeteta", "Njet_{eta}"],
                     [ "MonitorAnalyzer/n/npvs","-pvs", "N_{pv}" ] ]
    fileDesc = "mcVsdata-reweight"
#    if 'dataset' in onenode[1]:
#        fileDesc += '-%s' % onenode[1]['dataset']
#    if 'trigger' in onenode[1]:
#        fileDesc += '-%s' % onenode[1]['trigger']
    for oneMonitor in monitorNames:
        #root -b -q 'SingleComparison.C("edges/run_s8_monitor_input-RUN2010A-hltjet10u-60to80-TCHEM-noskip/output.root","MonitorAnalyzer/n/njet_pt","test2.png","this is a header")'
        count += 1
        targetFileNameAbs = os.path.join( subMonitorAbs, "%s%s.png" % ( fileDesc, oneMonitor[1] ) )
        targetFileNameRel = os.path.join( subMonitorRel, "%s%s.png" % ( fileDesc, oneMonitor[1] ) )
        targetFileNameRatioAbs = os.path.join( subMonitorAbs, "%s%s-ratio.png" % ( fileDesc, oneMonitor[1] ) )
        targetFileNameRatioRel = os.path.join( subMonitorRel, "%s%s-ratio.png" % ( fileDesc, oneMonitor[1] ) )
        targetFileNameAbsPdf = os.path.join( subMonitorAbs, "%s%s.pdf" % ( fileDesc, oneMonitor[1] ) )
        targetFileNameRelPdf = os.path.join( subMonitorRel, "%s%s.pdf" % ( fileDesc, oneMonitor[1] ) )
        targetFileNameRatioAbsPdf = os.path.join( subMonitorAbs, "%s%s-ratio.pdf" % ( fileDesc, oneMonitor[1] ) )
        targetFileNameRatioRelPdf = os.path.join( subMonitorRel, "%s%s-ratio.pdf" % ( fileDesc, oneMonitor[1] ) )

        print "Zipping %s to %s" % (onedata[0].getName(), onemc[0].getName() )
        getImage = LocalScriptEdge(
                            name = "extract-monitor-double-%s-%s" % (count, oneMonitor[1]) , 
                # big command #
                            command = \
                BindSubstitutes("root -b -q '%s(\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",%s,%s,\"%s\",\"%s\")'" % \
                                ( doubleComparisonMacro, '%s','%s', 
                                    oneMonitor[0], targetFileNameAbs,targetFileNameRatioAbs, oneMonitor[2], minbin, maxbin,
                                    dataLabel, mcLabel),
                                  [BindFunction( func = getFileNameStub,
                                                 args = { 'node': onedata[0] } ),
                                   BindFunction( func = getFileNameStub,
                                                 args = { 'node': onemc[0] } )] ),
                            output=targetFileNameAbs, 
                            noEmptyFiles=True)
        getImage.setWorkDir( subMonitorAbs )
        collectNode = Node( "collect-double-%s" % count )
        g.addNode( collectNode )
        g.addEdge( onedata[0], collectNode, NullEdge() )
        g.addEdge( onemc[0], collectNode, NullEdge() )

        g.addEdge( collectNode, doubleMonitorTarget, getImage )

        doubleMonitorPlotList.add( targetFileNameRel,# opoint = onedata[1]['opoint'], 
                                                     #b:in    = onedata[1]['bin'],
                                                      type   = oneMonitor[1],
                                                      ratiorel= targetFileNameRatioRel )
        getImage = LocalScriptEdge(
                            name = "extract-monitor-double-%s-%s-pdf" % (count, oneMonitor[1]) , 
                            # big command #
                            command = \
                BindSubstitutes("root -b -q '%s(\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",%s,%s,\"%s\",\"%s\")'" % \
                                ( doubleComparisonMacro, '%s','%s', 
                                    oneMonitor[0], targetFileNameAbsPdf,targetFileNameRatioAbsPdf, oneMonitor[2], minbin, maxbin,
                                    dataLabel, mcLabel),
                                  [BindFunction( func = getFileNameStub,
                                                 args = { 'node': onedata[0] } ),
                                   BindFunction( func = getFileNameStub,
                                                 args = { 'node': onemc[0] } )] ),
                            output=targetFileNameAbs, 
                            noEmptyFiles=True)


if 0:   
    output = "<html><head><title>dijet10 exercise</title></head><body><h1>dijet10U exercise</h1><hr />"
    output += "<p>MC is cut on the HLT_BTagMu_DiJet10U trigger, Data is Jet10U+Jet20U</p><hr />"
    #for onebin in doubleMonitorPlotList.iterGrouped( 'bin' ):
    #    currbin = onebin.vals[0][1]['bin']
    #    output += "<h2> bin %s </h2><hr />" % currbin
    for oneop in doubleMonitorPlotList.iterGrouped( 'opoint' ):
    #    currop  = oneop.vals[0][1]['opoint']
        currop = "test"
        output += "<h3>Operating point %s</h3>" % currop
        for onemon in oneop.iterMany():
            output += "<h4>%s</h4>\n" % onemon[1]['type']
            output += '<img src="%s" /> <img src="%s" />\n' % (onemon[0], onemon[1]['ratiorel'])
    output += "</body>"
    
    try:
        print output
        handle = open( webRoot + "/dump.html", "w+" )
        handle.write(output)
        handle.close()
    except:
        print "Couldn't do output"


def getGraph():
    global g
    return g

