package com.tinkerpop.blueprints.impls.tg;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import junit.framework.TestCase;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Index;
import com.tinkerpop.blueprints.Vertex;

/**
 * @author Victor Su
 */
public class TinkerMetadataWriterTest extends TestCase {

	public void testNormal() throws Exception {
		TinkerGraph g = TinkerGraphFactory.createTinkerGraph();
		createManualIndices(g);
		createKeyIndices(g);

		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		TinkerMetadataWriter.save(g, bos);

		TinkerGraph g2 = new TinkerGraph();
		TinkerMetadataReader.load(g2, new ByteArrayInputStream(bos.toByteArray()));


		assertEquals(g.currentId, g2.currentId);
		assertEquals(g.getIndexedKeys(Vertex.class), g2.getIndexedKeys(Vertex.class));
		assertEquals(g.getIndexedKeys(Edge.class), g2.getIndexedKeys(Edge.class));
		assertNotNull(g.getIndex("age", Vertex.class));
		assertNotNull(g.getIndex("weight", Edge.class));        
	}

	private void createKeyIndices(final TinkerGraph g) {
		g.createKeyIndex("name", Vertex.class);
		g.createKeyIndex("weight", Edge.class);
	}

	private void createManualIndices(final TinkerGraph g) {
		final Index<Vertex> idxAge = g.createIndex("age", Vertex.class);
		final Vertex v1 = g.getVertex(1);
		final Vertex v2 = g.getVertex(2);
		idxAge.put("age", v1.getProperty("age"), v1);
		idxAge.put("age", v2.getProperty("age"), v2);

		final Index<Edge> idxWeight = g.createIndex("weight", Edge.class);
		final Edge e7 = g.getEdge(7);
		final Edge e12 = g.getEdge(12);
		idxWeight.put("weight", e7.getProperty("weight"), e7);
		idxWeight.put("weight", e12.getProperty("weight"), e12);
	}

}
