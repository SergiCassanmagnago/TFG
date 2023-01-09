# Implementation of the Dynamic Pipeline WCC algorithm

*To execute dp_wcc.go in print mode, run:*

go run dpwcc.go input output print

where input is a file with the .requests extension where each line corresponds to an instruction, as depicted in the file examples.requests

dp_wcc.go will produce an output.wcc file containing the different weakly connected components of the graph depicted in network.requests

To execute dp_wcc.go in test mode, run:

go run dpwcc.go input output test

where input is a file with the .requests extension where each line corresponds to an instruction, as depicted in the file examples.requests

dp_wcc.go will produce a dpinputoutput.wcc file containing the traces of the generation of the weakly connected components of the graph depicted in network.requests
