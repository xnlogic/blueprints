package com.tinkerpop.blueprints.impls.neo4j2;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;
import com.tinkerpop.blueprints.util.DefaultVertexQuery;
import com.tinkerpop.blueprints.util.StringFactory;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Joey Freund
 * 
 * FIXME: When iterating in BOTH directions, we should merge two existing iterables (to get consistent behaviour with Blueprints test cases)
 */
public class Neo4j2Vertex extends Neo4j2Element implements Vertex {

	public Neo4j2Vertex(final Node node, final Neo4j2Graph graph) {
		super(graph);
		this.rawElement = node;

	}

	public Iterable<Edge> getEdges(final com.tinkerpop.blueprints.Direction direction, final String... labels) {
		return new Neo4jVertexEdgeIterable(direction, labels);
	}

	public Iterable<Vertex> getVertices(final com.tinkerpop.blueprints.Direction direction, final String... labels) {
		return new Neo4jVertexVertexIterable(direction, labels);
	}

	public Edge addEdge(final String label, final Vertex vertex) {
		return this.graph.addEdge(null, this, vertex, label);
	}

	public Collection<String> getLabels() {
		this.graph.autoStartTransaction(false);
		final Collection<String> labels = new ArrayList<String>();
		for (Label label : getRawVertex().getLabels()) {
			labels.add(label.name());
		}
		return labels;
	}

	public void addLabel(String label) {
		graph.autoStartTransaction(true);
		getRawVertex().addLabel(DynamicLabel.label(label));
	}

	public void removeLabel(String label) {
		graph.autoStartTransaction(true);
		getRawVertex().removeLabel(DynamicLabel.label(label));
	}

	public VertexQuery query() {
		this.graph.autoStartTransaction(false);
		return new DefaultVertexQuery(this);
	}

	public boolean equals(final Object object) {
		return object instanceof Neo4j2Vertex && ((Neo4j2Vertex) object).getId().equals(this.getId());
	}

	public String toString() {
		return StringFactory.vertexString(this);
	}

	public Node getRawVertex() {
		return (Node) this.rawElement;
	}

	
	
	private abstract class Neo4jVertexElementIterable<T extends Element> implements Iterable<T> {
		protected final Direction direction;
		protected final RelationshipType[] labels;
		
		public Neo4jVertexElementIterable(com.tinkerpop.blueprints.Direction direction, String... labels) {
			this.direction = toRawDirection(direction);
			this.labels = toRelationshipTypes(labels);
		}
		
		private Direction toRawDirection(com.tinkerpop.blueprints.Direction direction){
			switch (direction) {
			case OUT:
				return Direction.OUTGOING;
			case IN:
				return Direction.INCOMING;
			case BOTH:
				return Direction.BOTH;
			default:
				throw new RuntimeException("Unrecognized Blueprints direction - " + direction);
			}
		}
		
		private RelationshipType[] toRelationshipTypes(String... labels){
			RelationshipType[] relationshipTypes = new DynamicRelationshipType[labels.length];
			for (int i = 0; i < labels.length; i++) {
				relationshipTypes[i] = DynamicRelationshipType.withName(labels[i]);
			}
			return relationshipTypes;
		}
		
		protected Iterator<Relationship> getRelationshipIterator(){
			graph.autoStartTransaction(false);
			// This is the behaviour expected by Blueprint ...
			if(this.direction.equals(Direction.INCOMING) || this.direction.equals(Direction.OUTGOING)){
				return getRelationshipIterator(this.direction);
			} else {
				final Iterator<Relationship> incoming = getRelationshipIterator(Direction.INCOMING);
				final Iterator<Relationship> outgoing = getRelationshipIterator(Direction.OUTGOING);
				return new Iterator<Relationship>() {

					@Override
					public boolean hasNext() {
						return incoming.hasNext() || outgoing.hasNext();
					}

					@Override
					public Relationship next() {
						if(incoming.hasNext()){
							return incoming.next();
						} else {
							return outgoing.next();
						}
					}

					@Override
					public void remove() {
						throw new UnsupportedOperationException();
					}
				};
			}
		}
		
		
		private Iterator<Relationship> getRelationshipIterator(Direction direction){
			return labels.length > 0 ? getRawVertex().getRelationships(direction, labels).iterator() : 
				   getRawVertex().getRelationships(direction).iterator();
		}
		
		
	}


	private class Neo4jVertexVertexIterable extends Neo4jVertexElementIterable<Vertex>  implements Iterable<Vertex> {
		public Neo4jVertexVertexIterable(com.tinkerpop.blueprints.Direction direction, String[] labels) {
			super(direction, labels);
		}

		@Override
		public Iterator<Vertex> iterator() {
			return new VertexVertexIterator(getRelationshipIterator());
		}
	}
	
	
	private class Neo4jVertexEdgeIterable extends Neo4jVertexElementIterable<Edge>  implements Iterable<Edge> {
		public Neo4jVertexEdgeIterable(com.tinkerpop.blueprints.Direction direction, String[] labels) {
			super(direction, labels);
		}

		@Override
		public Iterator<Edge> iterator() {
			return new VertexEdgeIterator(getRelationshipIterator());
		}
	}
	
	
	
	private abstract class VertexElementIterator<T extends Element> implements Iterator<T> {
		protected Iterator<Relationship> relationshipIterator;

		public VertexElementIterator(Iterator<Relationship> relationshipIterator) {
			this.relationshipIterator = relationshipIterator;
		}
		
		@Override
		public boolean hasNext() {
			graph.autoStartTransaction(false);
			return this.relationshipIterator.hasNext();
		}
		
		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}
	
	
	
	private class VertexEdgeIterator extends VertexElementIterator<Edge> implements Iterator<Edge>{

		public VertexEdgeIterator(Iterator<Relationship> relationshipIterator) {
			super(relationshipIterator);
		}

		@Override
		public Edge next() {
			graph.autoStartTransaction(false);
			return new Neo4j2Edge(this.relationshipIterator.next(), graph);
		}
	}
	
	
	private class VertexVertexIterator extends VertexElementIterator<Vertex> implements Iterator<Vertex>{

		public VertexVertexIterator(Iterator<Relationship> relationshipIterator) {
			super(relationshipIterator);
		}

		@Override
		public Vertex next() {
			graph.autoStartTransaction(false);
			return new Neo4j2Vertex(this.relationshipIterator.next().getOtherNode(getRawVertex()), graph);
		}		
	}
	

}
