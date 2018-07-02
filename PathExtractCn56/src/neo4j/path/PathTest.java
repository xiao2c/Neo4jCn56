package neo4j.path;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpanders;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Uniqueness;

/**
 * This class is configured as the mainClass in pom.xml
 */
public class PathTest {
	/**
	 * Note: The neo4j database location is hard coded.
	 */
	private static final File databaseDirectory = new File("/opt/neo4j/data/databases/conceptNet5.6.db");

	GraphDatabaseService graphDb;

	/**
	 * All relationships in conceptNet
	 */
	private static enum Rel implements RelationshipType {
		Synonym, HasContext, IsA, HasProperty, DefinedAs, AtLocation, HasA, SimilarTo, Antonym, HasLastSubevent, NotDesires, CapableOf, PartOf, Desires, UsedFor, MadeOf, ReceivesAction, InstanceOf, NotHasProperty, CausesDesire, HasPrerequisite, HasSubevent, Causes, MotivatedByGoal, MannerOf, DistinctFrom, Entails, NotCapableOf, CreatedBy, LocatedNear
	}

	/**
	 * @param args args[0], Input text file contains list of all words to be processed; args[1], Max depth of the path (e.g. 5); args[2], Directory where out put file will be saved.
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		PathTest test = new PathTest();
		test.testDb(args[0], args[1], args[2]);
		test.shutDown();
	}

	void testDb(String filename, String dep, String rootDir) throws IOException {
		graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(databaseDirectory);
		registerShutdownHook(graphDb);

		File f = new File(filename);
		BufferedReader b = new BufferedReader(new FileReader(f));
		String line = "";
		int depth = Integer.parseInt(dep);
		while ((line = b.readLine()) != null) {
			processWord(line.trim(), depth, rootDir);
		}
		b.close();
	}

	void processWord(String word, int depth, String rootDir) throws IOException {
		long startTime = System.currentTimeMillis();
		System.out.println("(" + word + ") START AT: " + new Date(startTime));

		try (Transaction tx = graphDb.beginTx()) {
			int count = 0;
			ArrayList<Pair<Double, Path>> topPaths = new ArrayList<>();
			Node node = graphDb.findNode(DynamicLabel.label("Concept"), "uri", "/c/en/" + word); // Start node of the path.
			if (null != node) {
				Direction dire = Direction.OUTGOING; // Fixed direction of the path: Direction.OUTGOING, Direction.INCOMING
				/*
				// Use specific list of types relation
				TraversalDescription travDesc = graphDb.traversalDescription()
						.expand(PathExpanders.forTypesAndDirections(Rel.Synonym, dire, Rel.HasContext, dire, Rel.IsA, dire, Rel.HasProperty, dire,
								Rel.DefinedAs, dire, Rel.AtLocation, dire, Rel.HasA, dire, Rel.SimilarTo, dire, Rel.Antonym, dire, Rel.HasLastSubevent, dire,
								Rel.NotDesires, dire, Rel.CapableOf, dire, Rel.PartOf, dire, Rel.Desires, dire, Rel.UsedFor, dire, Rel.MadeOf, dire,
								Rel.ReceivesAction, dire, Rel.InstanceOf, dire, Rel.NotHasProperty, dire, Rel.CausesDesire, dire, Rel.HasPrerequisite, dire,
								Rel.HasSubevent, dire, Rel.Causes, dire, Rel.MotivatedByGoal, dire, Rel.MannerOf, dire, Rel.DistinctFrom, dire, Rel.Entails,
								dire, Rel.NotCapableOf, dire, Rel.CreatedBy, dire, Rel.LocatedNear, dire))
						.depthFirst().evaluator(Evaluators.toDepth(depth)).uniqueness(Uniqueness.NODE_PATH);
				*/
				// Use any type of relation
				TraversalDescription travDesc = graphDb.traversalDescription().expand(PathExpanders.forDirection(dire)).depthFirst().evaluator(Evaluators.toDepth(depth)).uniqueness(Uniqueness.NODE_PATH);
				for (Path position : travDesc.traverse(node)) {

					if (position.length() <= 0) {
						continue;
					}

					double totalWeight = 0.;
					for (Relationship rel : position.relationships()) {
						totalWeight += (double) rel.getProperty("w");
					}
					double avgWeight = totalWeight / position.length();

					// loop the tops
					Pair<Double, Path> np = Pair.of(avgWeight, position);

					int sizeBeforeInsert = topPaths.size();
					boolean inserted = false;
					for (int i = 0; i < sizeBeforeInsert; i++) {
						Pair<Double, Path> p = topPaths.get(i);
						if (np.getLeft() >= p.getLeft()) {
							topPaths.add(i, np);
							inserted = true;
							if (sizeBeforeInsert >= 10) { // Remove the last node that been pushed to index 10.
								topPaths.remove(sizeBeforeInsert);
							}
							break;
						}
					}
					if (!inserted && topPaths.size() < 10) {
						topPaths.add(np); // The topPaths list has less than 10 element, append this to end.
					}

					++count;
				}
				System.out.println(count);

				String rows = "avgWeight\tPath\n";
				for (Pair<Double, Path> p : topPaths) {
					rows += printPair(p) + "\n";
				}
				Files.write(Paths.get(rootDir + "/" + word + "_" + depth + ".out"), rows.getBytes());
			} else {
				Files.write(Paths.get(rootDir + "/" + word + "_" + depth + ".none"), ("No such concept (" + word + ")").getBytes());
			}

			tx.success();
		}

		long stopTime = System.currentTimeMillis();
		float runTime = (stopTime - startTime) / 1000f;
		System.out.println("(" + word + ") END AT: " + new Date(stopTime));
		System.out.println("Run time: " + runTime + " sec\n\n");
	}

	String printPair(Pair<Double, Path> p) {
		String rows = p.getLeft().doubleValue() + "\t";
		Iterator<PropertyContainer> itr = ((Path) p.getRight()).iterator();
		long nodeId = -1;
		while (itr.hasNext()) {
			PropertyContainer x = (PropertyContainer) itr.next();
			if (x instanceof Node) {
				Node node = (Node) x;
				rows += "(" + node.getProperty("name") + ", " + node.getId() + ")";
				nodeId = node.getId();
			} else if (x instanceof Relationship) {
				Relationship r = (Relationship) x;
				if (r.getStartNode().getId() == nodeId) {
					rows += " >"; // Indicate outgoing relationship
				} else {
					rows += " <"; // Indicate incoming relationship
				}
				rows += "[" + r.getType().name() + ", " + r.getId() + ", " + r.getProperty("w") + "] ";
			}
		}
		System.out.println(rows);
		return rows;
	}

	void shutDown() {
		System.out.println();
		System.out.println("Shutting down database ...");
		graphDb.shutdown();
	}

	private static void registerShutdownHook(final GraphDatabaseService graphDb) {
		// Registers a shutdown hook for the Neo4j instance so that it
		// shuts down nicely when the VM exits (even if you "Ctrl-C" the
		// running application).
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				graphDb.shutdown();
			}
		});
	}
}