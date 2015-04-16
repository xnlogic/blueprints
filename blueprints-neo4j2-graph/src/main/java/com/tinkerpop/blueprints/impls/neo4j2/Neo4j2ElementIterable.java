package com.tinkerpop.blueprints.impls.neo4j2;

import java.util.Iterator;

import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.index.IndexHits;

import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Element;

/**
 * 
 * @author Joey Freund
 *
 * Given an Iterable of either Node or Relationship, return an Iterable of either Vertex or Edge.
 * 
 * @param <T> Either Node or Relationship     (Neo4j interfaces)
 * @param <S> Either Neo4jVertex or Neo4jEdge (Blueprints implementations)
 */
public class Neo4j2ElementIterable<T extends PropertyContainer, S extends Element> implements CloseableIterable<S> {

    private final Iterable<T> elements;
    private final Neo4j2Graph graph;

    public Neo4j2ElementIterable(final Iterable<T> elements, final Neo4j2Graph graph) {
        this.graph = graph;
        this.elements = elements;
    }

	
	@Override
	public Iterator<S> iterator() {
		return new Neo4j2ElementIterator<T, S>(this.elements, this.graph);
	}

	
	@Override
	public void close() {
		if (this.elements instanceof IndexHits) {
            ((IndexHits<T>) this.elements).close();
        }
	}

}
