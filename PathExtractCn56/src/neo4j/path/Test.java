package neo4j.path;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.cypher.internal.ExecutionEngine;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.io.fs.FileUtils;

/**
 * @author chenyi
 * This class make use of cypher query on conceptNet to extract required paths.
 */
public class Test {
	private static final File databaseDirectory = new File("/opt/neo4j/data/databases/conceptNet5.6.db");

	public String greeting;

	// START SNIPPET: vars
	GraphDatabaseService graphDb;
	Node firstNode;
	Node secondNode;
	Relationship relationship;
	// END SNIPPET: vars

	public static void main(final String[] args) throws IOException {
		Test test = new Test();
		test.testDb(args[0]);
		test.shutDown();
	}

	void testDb(String query) throws IOException {
		// FileUtils.deleteRecursively( databaseDirectory );

		// START SNIPPET: startDb
		graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(databaseDirectory);
		registerShutdownHook(graphDb);
		// END SNIPPET: startDb

		long startTime = System.currentTimeMillis();
		System.out.println("START AT: " + new Date(startTime));
		try (Transaction ignored = graphDb.beginTx();
				//Result result = graphDb.execute("match p=(n{name:'"+word+"'})-[r*1.."+max+"]-(m) where n<>m with p, r unwind r as re return p, sum(re.w) as wsum order by wsum desc limit 10")) {
				Result result = graphDb.execute(query)) {

			while (result.hasNext()) {
				Map<String, Object> row = result.next();
				String rows = "";
				double avgw = (Double)row.get("avgw");
				Path path = (Path)row.get("p");

				rows += avgw + "\t";
				Iterator<PropertyContainer> itr = path.iterator();
				long nodeId = -1;
				while (itr.hasNext()) {
					PropertyContainer x = (PropertyContainer) itr.next();
					if(x instanceof Node) {
						Node node = (Node) x;
						rows += "("+node.getProperty("name")+", "+node.getId()+")";
						nodeId = node.getId();
					} else if(x instanceof Relationship) {
						Relationship r = (Relationship) x;
						if(r.getStartNode().getId() == nodeId) {
							rows += " >";
						} else {
							rows += " <";
						}
						rows += "["+r.getType().name()+", "+r.getId()+", "+r.getProperty("w")+"] ";
					}
				}
				System.out.println(rows);
			}
		}		
		
		long stopTime = System.currentTimeMillis();
		long runTime = stopTime - startTime;
		System.out.println("END AT: " + new Date(startTime));
		System.out.println("Run time: " + runTime);

		/*
		// START SNIPPET: transaction
		try (Transaction tx = graphDb.beginTx()) {
			// Database operations go here
			// END SNIPPET: transaction
			// START SNIPPET: addData
			firstNode = graphDb.createNode();
			firstNode.setProperty("message", "Hello, ");
			secondNode = graphDb.createNode();
			secondNode.setProperty("message", "World!");

			relationship = firstNode.createRelationshipTo(secondNode, RelTypes.KNOWS);
			relationship.setProperty("message", "brave Neo4j ");
			// END SNIPPET: addData

			// START SNIPPET: readData
			System.out.print(firstNode.getProperty("message"));
			System.out.print(relationship.getProperty("message"));
			System.out.print(secondNode.getProperty("message"));
			// END SNIPPET: readData

			greeting = ((String) firstNode.getProperty("message")) + ((String) relationship.getProperty("message"))
					+ ((String) secondNode.getProperty("message"));

			// START SNIPPET: transaction
			tx.success();
		}
		// END SNIPPET: transaction
		*/
	}

	void shutDown() {
		System.out.println();
		System.out.println("Shutting down database ...");
		// START SNIPPET: shutdownServer
		graphDb.shutdown();
		// END SNIPPET: shutdownServer
	}

	// START SNIPPET: shutdownHook
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
	// END SNIPPET: shutdownHook
}