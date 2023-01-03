/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package afwcc;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.*;
import java.util.*;

import static org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE;

public class DataStreamJob {

	public static void main(String[] args) throws Exception {

		// Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);

		// Maximum iterations of the algorithm
		final int maxIterations = params.getInt("iterations", 10);

		// Setting up the execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		//Reading from the input file and storing it in a list of tuples
		File in = new File(params.get("input"));
		List<Tuple2<Integer, Integer>> edgeTuples = getEdges(in);

		DataSet<Tuple2<Integer, Integer>> edges = env.fromCollection(edgeTuples).flatMap(new UndirectEdge());
		DataSet<Integer> vertices = edges.flatMap(new CollectVertex()).distinct();
		DataSet<Tuple2<Integer, Integer>> verticesWithInitialId = vertices.map(new AssignID());

		// open a delta iteration
		DeltaIteration<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> iteration =
				verticesWithInitialId.iterateDelta(verticesWithInitialId, maxIterations, 0);

		// apply the step logic: join with the edges, select the minimum neighbor, update if the
		// component of the candidate is smaller
		DataSet<Tuple2<Integer, Integer>> changes =
				iteration
						.getWorkset()
						.join(edges)
						.where(0)
						.equalTo(0)
						.with(new NeighborWithComponentIDJoin())
						.groupBy(0)
						.aggregate(Aggregations.MIN, 1)
						.join(iteration.getSolutionSet())
						.where(0)
						.equalTo(0)
						.with(new ComponentIdFilter());

		// close the delta iteration (delta and new work set are identical)
		DataSet<HashSet> cc = iteration.closeWith(changes, changes)
				.groupBy(1)
				.reduceGroup(new ConnectedComponents());

		if (params.has("output")) {
			List<HashSet> results = cc.collect();
			OutputFile(params.get("output"), results);
			cc.writeAsText(params.get("output"), OVERWRITE).setParallelism(1);

			// execute program
			env.execute("Connected Components");
		} else throw new java.lang.RuntimeException("Use --output to specify output path\n");
	}

	public static List<Tuple2<Integer, Integer>> getEdges(File file) throws FileNotFoundException {
		List<Tuple2<Integer, Integer>> edges = new ArrayList<>();

		Scanner scan = new Scanner(file);
		int vertex1;
		int vertex2;
		while (scan.hasNextLine()){
			vertex1 = scan.nextInt();
			vertex2 = scan.nextInt();
			edges.add(Tuple2.of(vertex1, vertex2));
		}
		return edges;
	}

	public static final class CollectVertex implements FlatMapFunction<Tuple2<Integer, Integer>, Integer> {
		@Override
		public void flatMap(Tuple2<Integer, Integer> value, Collector<Integer> out) {
			out.collect(value.f0);
			out.collect(value.f1);
		}
	}

	@FunctionAnnotation.ForwardedFields("*->f0")
	public static final class AssignID<T> implements MapFunction<T, Tuple2<T, T>> {

		@Override
		public Tuple2<T, T> map(T vertex) {
			return new Tuple2<>(vertex, vertex);
		}
	}


	/**
	 * Undirected edges by emitting for each input edge the input edges itself and an inverted
	 * version.
	 */
	public static final class UndirectEdge
			implements FlatMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {
		Tuple2<Integer, Integer> invertedEdge = new Tuple2<>();

		@Override
		public void flatMap(Tuple2<Integer, Integer> edge, Collector<Tuple2<Integer, Integer>> out) {
			invertedEdge.f0 = edge.f1;
			invertedEdge.f1 = edge.f0;
			out.collect(edge);
			out.collect(invertedEdge);
		}
	}

	/**
	 * UDF that joins a (Vertex-ID, Component-ID) pair that represents the current component that a
	 * vertex is associated with, with a (Source-Vertex-ID, Target-VertexID) edge. The function
	 * produces a (Target-vertex-ID, Component-ID) pair.
	 */
	@FunctionAnnotation.ForwardedFieldsFirst("f1->f1")
	@FunctionAnnotation.ForwardedFieldsSecond("f1->f0")
	public static final class NeighborWithComponentIDJoin implements JoinFunction
			<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

		@Override
		public Tuple2<Integer, Integer> join(
				Tuple2<Integer, Integer> vertexWithComponent, Tuple2<Integer, Integer> edge) {
			return new Tuple2<>(edge.f1, vertexWithComponent.f1);
		}
	}

	/**
	 * Emit the candidate (Vertex-ID, Component-ID) pair if and only if the candidate component ID
	 * is less than the vertex's current component ID.
	 */
	@FunctionAnnotation.ForwardedFieldsFirst("*")
	public static final class ComponentIdFilter implements FlatJoinFunction
			<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

		@Override
		public void join(
				Tuple2<Integer, Integer> candidate,
				Tuple2<Integer, Integer> old,
				Collector<Tuple2<Integer, Integer>> out) {
			if (candidate.f1 < old.f1) {
				out.collect(candidate);
			}
		}
	}

	public static class ConnectedComponents
			implements GroupReduceFunction<Tuple2<Integer, Integer>, HashSet> {

		@Override
		public void reduce(Iterable<Tuple2<Integer, Integer>> iterable, Collector<HashSet> collector) {

			HashSet<Integer> vertexes = new HashSet<>();

			// add all vertexes of the group to the set
			for (Tuple2<Integer, Integer> t : iterable) {
				vertexes.add(t.f0);
			}
			collector.collect(vertexes);
		}
	}

	public static void OutputFile(String file, List<HashSet> results) throws IOException {
		PrintWriter writer = new PrintWriter(file);
		writer.print("");
		writer.close();

		FileWriter fw = new FileWriter(file,true);
		for (HashSet r : results){
			fw.write(r + "\n");
		}
		fw.close();
	}
}
