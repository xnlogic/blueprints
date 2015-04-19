package com.tinkerpop.blueprints.impls.neo4j2;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;

import com.tinkerpop.blueprints.Element;

/**
 *
 * @author Joey Freund
 *
 * Given an Iterable of either Node or Relationship, return an iterator that:
 * 1. Wraps each Node or Relationship with a Neo4j2Vertex or Neo4j2Edge, respectively.
 * 2. Skips nodes/relationships that have been deleted in the current transaction (which may or may not have not been committed).
 *
 * @param <T> Either Node or Relationship     (Neo4j interfaces)
 * @param <S> Either Neo4jVertex or Neo4jEdge (Blueprints implementations)
 */
public class Neo4j2ElementIterator<T extends PropertyContainer, S extends Element> implements Iterator<S>{
	
	public static interface SkipCondition<T extends PropertyContainer> {
		public boolean shouldSkip(T element);
	}
	
	private final Iterator<T> elementIterator;
	private final Neo4j2Graph graph;
    private T nextElement = null;
    private SkipCondition<T> skipCondition;
    
    public Neo4j2ElementIterator(final Iterable<T> elements, final Neo4j2Graph graph, SkipCondition<T> skipCondition) {
		this.elementIterator = elements.iterator();
		this.graph = graph;
		this.skipCondition = skipCondition;
		fetchNextElement();
	}
    
    public Neo4j2ElementIterator(final Iterable<T> elements, final Neo4j2Graph graph) {
    	this(elements, graph, null);
    }
    
    protected boolean isDeleted(PropertyContainer element){
    	try {
			element.getProperty("dummy", null);
			return false;
		} catch (IllegalStateException e) {
			return true;
		}
    }
    
    
    private void fetchNextElement(){
    	nextElement = null;
    	
    	while(elementIterator.hasNext()){
    		T element = elementIterator.next();
    		if(skipCondition != null && skipCondition.shouldSkip(element)){
    			continue;
    		}
    		if(! isDeleted(element)){
    			nextElement = element;
    			break;
    		}
    	}
    }

    
    @Override
    public void remove() {
        this.elementIterator.remove();
    }

    
    @SuppressWarnings("unchecked")
	@Override
    public S next() {
        graph.autoStartTransaction(false);
        
        if(nextElement == null){
        	throw new NoSuchElementException();
        }
        
        Neo4j2Element result = null; 
        if(this.nextElement instanceof Node){
        	result = new Neo4j2Vertex((Node) this.nextElement, graph);
        } else if (this.nextElement instanceof Relationship){
        	result = new Neo4j2Edge((Relationship) this.nextElement, graph);
        }
        
        fetchNextElement();
        return (S) result;
    }

    @Override
    public boolean hasNext() {
        return this.nextElement != null;
    }
    
}
