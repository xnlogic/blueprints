package com.tinkerpop.blueprints.impls.neo4j2;


import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ElementHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
abstract class Neo4j2Element implements Element {

	protected final Neo4j2Graph graph;
	protected PropertyContainer rawElement;

	public Neo4j2Element(final Neo4j2Graph graph) {
		this.graph = graph;
	}

	
	@SuppressWarnings("unchecked")
	public <T> T getProperty(final String key) {
		this.graph.autoStartTransaction(false);
		if (this.rawElement.hasProperty(key)){
			return (T) tryConvertArrayToArrayList(this.rawElement.getProperty(key));
		} else {
			return null;
		}
	}

	@Override
	public void setProperty(final String key, Object value) {
		ElementHelper.validateProperty(this, key, value);
		value = convertToArrayIfNecessary(value);   // If the value is a collection, convert it to an Array, so that Neo4j can consume it.
		validateSupportedValueType(value);

		this.graph.autoStartTransaction(true);
		this.rawElement.setProperty(key, value);
	}
	
	@SuppressWarnings("unchecked")
	public <T> T removeProperty(final String key) {
		if (!this.rawElement.hasProperty(key))
			return null;
		else {
			this.graph.autoStartTransaction(true);
			return (T) this.rawElement.removeProperty(key);
		}
	}

	public Set<String> getPropertyKeys() {
		this.graph.autoStartTransaction(false);
		final Set<String> keys = new HashSet<String>();
		for (final String key : this.rawElement.getPropertyKeys()) {
			keys.add(key);
		}
		return keys;
	}

	
	
	public int hashCode() {
		return this.getId().hashCode();
	}

	public PropertyContainer getRawElement() {
		return this.rawElement;
	}

	public Object getId() {
		this.graph.autoStartTransaction(false);
		if (this.rawElement instanceof Node) {
			return ((Node) this.rawElement).getId();
		} else {
			return ((Relationship) this.rawElement).getId();
		}
	}

	public void remove() {
		if (this instanceof Vertex)
			this.graph.removeVertex((Vertex) this);
		else
			this.graph.removeEdge((Edge) this);
	}

	public boolean equals(final Object object) {
		return ElementHelper.areEqual(this, object);
	}


	//-------------------------------------------------------------------------
	// Private helpers ...
	
	
	// NOTE: We want to perform this validation, before setting the property's value.
	//       If an exception is thrown, while setting a property value, there is a side-effect on the current transaction (it is marked as rollbackOnly).
	private void validateSupportedValueType(Object value){
		// Check that the value is of one of the supported types ...
		if (value instanceof Boolean || value instanceof Boolean[] || value instanceof boolean[] ||
				value instanceof Byte || value instanceof Byte[] || value instanceof byte[] ||
				value instanceof Short || value instanceof Short[] || value instanceof short[] ||
				value instanceof Integer || value instanceof Integer[] || value instanceof int[] ||
				value instanceof Long || value instanceof Long[] || value instanceof long[] ||
				value instanceof Float || value instanceof Float[] || value instanceof float[] ||
				value instanceof Double || value instanceof Double[] || value instanceof double[] ||
				value instanceof Character || value instanceof Character[] || value instanceof char[] ||
				value instanceof String || value instanceof String[]){
			return;
		}

		throw new IllegalArgumentException("Property values of type " + value.getClass().getCanonicalName() + " are not allowed.");
	}
	
	
	private Object convertToArrayIfNecessary(final Object value) {
		// If the value is not a collection, no need to convert ...
		if (!(value instanceof Collection<?>)){
			return value;
		}

		// Otherwise, try to convert the collection to an array ...
		try {
			final Collection<?> collection = (Collection<?>) value;
			Object[] array = null;
			final Iterator<?> objects = collection.iterator();
			for (int i = 0; objects.hasNext(); i++) {
				Object object = objects.next();
				if (array == null) {
					array = (Object[]) Array.newInstance(object.getClass(), collection.size());
				}
				array[i] = object;
			}
			return array;
		} catch (final ArrayStoreException ase) {
			throw new IllegalArgumentException("Neo4j doesn't support mixed-type collections as property values.");
		}
	}


	/**
	 * If <code>value</code> is an array, return an ArrayList object containing the same items.
	 * Otherwise, return <code>value</code>.
	 */
	private Object tryConvertArrayToArrayList(final Object value) {
		if(! value.getClass().isArray()){
			return value;
		}

		ArrayList<Object> list = new ArrayList<Object>();
		int arrlength = Array.getLength(value);
		for (int i = 0; i < arrlength; i++) {
			list.add(Array.get(value, i));
		}
		return list;
	}

}
