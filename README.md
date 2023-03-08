# TFG
This Repository is for gathering the code and results for the Master Thesis in Master on Innovation and Research in Advance Computing at [UPC](https://www.fib.upc.edu/en/studies/bachelors-degrees/bachelor-degree-informatics-engineering)
Implementation of data algorithms using the Dynamic Pipeline Paradigm and the Apache Flink framework 


and it consists on the benchmarking the continuous behavior of stream processing techniques on the problem of enumerating weakly connected components.

In this work, as a proof of concept, we conduct a comparative study of the continuous behavior of two stream processing-based solutions for this problem.

The first of the two approaches corresponds to the DP, which is a Paradigm that has been recently proposed as an approach to address SP, and it is currently still restricted to the academical environment. A DP is inherently incremental, Since it requires support for parallelization and communication between processes, we decided to use the Golang programming language for its implementation, since it offers native support for parallelization with its goroutines and communication between processes with its channels.

We finally decided to use Flink, with main reason being that, unlike Kafka and Spark, Flink supports both stream and batch processing. Given that an initial objective of this study was to be able to expand to cover both techniques in case there was time availability, using Flink gave us the ability to potentially cover both techniques with the same tool.

The next part of this thesis was the conduction of an experimental study between the two previously mentioned approaches. For this study,  we selected the graphs that are shown in the table from the Stanford DataSet Collection. The reason for selecting these particular datasets was to test both approaches in large complex and undirected graphs, with different amounts of nodes, edges, diameters and average clustering coefficients as shown in the table.

Regarding the experimental study, all graphs have been executed in a machine with an x86 architecture with 64 bits, an Intel Core i7 processor with 4 cores and 8 threads @ 1.80 GHz. 

When it comes to software, the approaches I have just mentioned have been implemented using golang 1.19.2 for the DP and Java version 11.0.7 and flink 1.16.0 for flink.

To assess the incremental delivery of results, we used the diefpy library, which is a python library used for measuring the incremental efficiency of different implementations. This library receives answer traces, which are files containing, for each of the produced results, the test that has been used along with the used approach, the answer number and the elapsed time for each particular answer.

Diefpy provides plots useful for comparing the total execution time of different executions as shown in this figure, as well as the time in which each answer has been generated as shown in this figure. 

It also allows measuring diefficiency metrics, i.e., the continuous efficiency of each implementation for generating incremental results, along with other metrics as shown in this radar plot. 

Based on the results of the empirical study we consider that a re-visiting of the implementation of the DP-WCC must be done in order to guarantee a convenient balance of the charge of each Filter instance. Additionally, to conduct new experiments considering datasets with different topologies, densities and vertices distribution along their connected components will give insights to answer the main question underlying the research of the Dynamic Pipeline Paradigm: Which family of problems favor this paradigm? We think that problems needing incremental emission of results are in this family but it is necessary to characterize them in deep. Finally, it is interesting to implement a dynamic pipeline using the Apache Flink Stateful Function API when it is consolidated and its documentation and resources are available. Stateful Functions seems to be a natural feature to implement stages defined in the Dynamic Pipeline Paradigm. Finally, it is interesting to implement a dynamic pipeline using the Apache Flink Stateful Function API when it is consolidate and documentation and resources.
