package com.tinkerpop.blueprints.impls.neo4j2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.neo4j.cypher.ExecutionEngine;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.MultipleFoundException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.AutoIndexer;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.helpers.Settings;
import org.neo4j.tooling.GlobalGraphOperations;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Index;
import com.tinkerpop.blueprints.IndexableGraph;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.MetaGraph;
import com.tinkerpop.blueprints.Parameter;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.DefaultGraphQuery;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.KeyIndexableGraphHelper;
import com.tinkerpop.blueprints.util.PropertyFilteredIterable;
import com.tinkerpop.blueprints.util.StringFactory;

/**
 * A Blueprints implementation of the graph database Neo4j (http://neo4j.org)
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Neo4j2Graph implements TransactionalGraph, IndexableGraph, KeyIndexableGraph, MetaGraph<GraphDatabaseService> {
	
    private static final Logger logger = Logger.getLogger(Neo4j2Graph.class.getName());

    
    private static GraphDatabaseBuilder createBuilder(String directory, Map<String, String> configuration){
        GraphDatabaseBuilder builder = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(directory);
        if (null != configuration){
                for(String key: configuration.keySet()){
                        Settings.setting(key, Settings.STRING, configuration.get(key));
                }
        }
        return builder;
    }


    private GraphDatabaseService rawGraph;
    
    
    public enum InternallyUsedLabels implements Label { Blueprints_GraphProperties };
    private static final String PROPERTY_KEY_AUTO_INDEXED_VERTEX_KEYS = "AUTO_INDEXED_VERTEX_KEYS";
    private static final String PROPERTY_KEY_AUTO_INDEXED_EDGE_KEYS = "AUTO_INDEXED_EDGE_KEYS";
    
    

    protected final ThreadLocal<Transaction> tx = new ThreadLocal<Transaction>() {
        protected Transaction initialValue() {
            return null;
        }
    };

    protected final ThreadLocal<Boolean> checkElementsInTransaction = new ThreadLocal<Boolean>() {
        protected Boolean initialValue() {
            return false;
        }
    };

    private static final Features FEATURES = new Features();

    static {

        FEATURES.supportsSerializableObjectProperty = false;
        FEATURES.supportsBooleanProperty = true;
        FEATURES.supportsDoubleProperty = true;
        FEATURES.supportsFloatProperty = true;
        FEATURES.supportsIntegerProperty = true;
        FEATURES.supportsPrimitiveArrayProperty = true;
        FEATURES.supportsUniformListProperty = true;
        FEATURES.supportsMixedListProperty = false;
        FEATURES.supportsLongProperty = true;
        FEATURES.supportsMapProperty = false;
        FEATURES.supportsStringProperty = true;

        FEATURES.supportsDuplicateEdges = true;
        FEATURES.supportsSelfLoops = true;
        FEATURES.isPersistent = true;
        FEATURES.isWrapper = false;
        FEATURES.supportsVertexIteration = true;
        FEATURES.supportsEdgeIteration = true;
        FEATURES.supportsVertexIndex = true;
        FEATURES.supportsEdgeIndex = true;
        FEATURES.ignoresSuppliedIds = true;
        FEATURES.supportsTransactions = true;
        FEATURES.supportsIndices = true;
        FEATURES.supportsKeyIndices = true;
        FEATURES.supportsVertexKeyIndex = true;
        FEATURES.supportsEdgeKeyIndex = true;
        FEATURES.supportsEdgeRetrieval = true;
        FEATURES.supportsVertexProperties = true;
        FEATURES.supportsEdgeProperties = true;
        FEATURES.supportsThreadedTransactions = false;
        FEATURES.supportsThreadIsolatedTransactions = true;
    }

    
    private final ExecutionEngine cypher;

    protected boolean checkElementsInTransaction() {
        if (this.tx.get() == null) {
            return false;
        } else {
            return this.checkElementsInTransaction.get();
        }
    }

    /**
     * Neo4j's transactions are not consistent between the graph and the graph
     * indices. Moreover, global graph operations are not consistent. For
     * example, if a vertex is removed and then an index is queried in the same
     * transaction, the removed vertex can be returned. This method allows the
     * developer to turn on/off a Neo4j2Graph 'hack' that ensures transactional
     * consistency. The default behavior for Neo4j2Graph is to use Neo4j's native
     * behavior which ensures speed at the expensive of consistency. Note that
     * this boolean switch is local to the current thread (i.e. a ThreadLocal
     * variable).
     *
     * @param checkElementsInTransaction check whether an element is in the transaction between
     *                                   returning it
     */
    public void setCheckElementsInTransaction(final boolean checkElementsInTransaction) {
        this.checkElementsInTransaction.set(checkElementsInTransaction);
    }

    public Neo4j2Graph(final String directory) {
        this(directory, null);
    }
    
    public Neo4j2Graph(final Configuration configuration) {
        this(configuration.getString("blueprints.neo4j.directory", null),
                ConfigurationConverter.getMap(configuration.subset("blueprints.neo4j.conf")));
    }
    
    public Neo4j2Graph(final String directory, final Map<String, String> configuration) {
    	this(createBuilder(directory, configuration).newGraphDatabase());
    }

    public Neo4j2Graph(final GraphDatabaseService rawGraph) {
        try{
        	this.rawGraph = rawGraph;
        	cypher = new ExecutionEngine(rawGraph, null);
            init();
        } catch (RuntimeException e) {
    		if (this.rawGraph != null)
                this.rawGraph.shutdown();
    		throw e;
        } 
        
    }


    protected void init() { 
        this.loadKeyIndices();
    }

    
    
    private void persistVertexAutoIndexerKeys(){
    	persistAutoIndexerKeys(rawGraph.index().getNodeAutoIndexer(), PROPERTY_KEY_AUTO_INDEXED_VERTEX_KEYS);
    }
    
    private void persistEdgeAutoIndexerKeys(){
    	persistAutoIndexerKeys(rawGraph.index().getRelationshipAutoIndexer(), PROPERTY_KEY_AUTO_INDEXED_EDGE_KEYS);
    }
    
    private <T extends PropertyContainer> void persistAutoIndexerKeys(AutoIndexer<T> indexer, String propertyKey){
    	this.autoStartTransaction(true);
    	getGraphProperties().setProperty(propertyKey, indexer.getAutoIndexedProperties().toArray(new String[]{}));
    }
    
    
    private <T extends Element> void loadKeyIndices(String propertyKey, Class<T> element){
    	String[] indexedKeys = (String[]) getGraphProperties().getProperty(propertyKey, new String[]{});
    	for (int i = 0; i < indexedKeys.length; i++) {
			createKeyIndex(indexedKeys[i], element);
		}
    }
    
    
    private void loadKeyIndices() {
    	loadKeyIndices(PROPERTY_KEY_AUTO_INDEXED_VERTEX_KEYS, Vertex.class);
    	loadKeyIndices(PROPERTY_KEY_AUTO_INDEXED_EDGE_KEYS, Edge.class);
    }
    
    
    
    /**
     * @return A property container that persists properties of the graph (instead of a specific node/relationship).
     */
    protected PropertyContainer getGraphProperties(){
    	// This simple implementation stores the graph properties in a vertex with a known label.
    	
    	this.autoStartTransaction(false);
		ResourceIterator<Node> nodes = this.rawGraph.findNodes(InternallyUsedLabels.Blueprints_GraphProperties);
		// Need to make sure there is only one such vertex ...
		
		PropertyContainer result = null;
		if(! nodes.hasNext()){ // Need to create the vertex ...
			this.autoStartTransaction(true);
			result = this.rawGraph.createNode(InternallyUsedLabels.Blueprints_GraphProperties);
			this.commit();
			this.autoStartTransaction(false);
		} else {
			result = nodes.next();
		}
		
		if(nodes.hasNext()){
			throw new MultipleFoundException("There is more than one vertex with the label " + InternallyUsedLabels.Blueprints_GraphProperties);
		}
		
    	return result;
    }
    


    public synchronized <T extends Element> Index<T> createIndex(final String indexName, final Class<T> indexClass, final Parameter... indexParameters) {
        this.autoStartTransaction(true);
        if (this.rawGraph.index().existsForNodes(indexName) || this.rawGraph.index().existsForRelationships(indexName)) {
            throw ExceptionFactory.indexAlreadyExists(indexName);
        }
        return new Neo4j2Index(indexName, indexClass, this, indexParameters);
    }

    public <T extends Element> Index<T> getIndex(final String indexName, final Class<T> indexClass) {
        this.autoStartTransaction(false);
        if (Vertex.class.isAssignableFrom(indexClass)) {
            if (this.rawGraph.index().existsForNodes(indexName)) {
                return new Neo4j2Index(indexName, indexClass, this);
            } else if (this.rawGraph.index().existsForRelationships(indexName)) {
                throw ExceptionFactory.indexDoesNotSupportClass(indexName, indexClass);
            } else {
                return null;
            }
        } else if (Edge.class.isAssignableFrom(indexClass)) {
            if (this.rawGraph.index().existsForRelationships(indexName)) {
                return new Neo4j2Index(indexName, indexClass, this);
            } else if (this.rawGraph.index().existsForNodes(indexName)) {
                throw ExceptionFactory.indexDoesNotSupportClass(indexName, indexClass);
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    /**
     * {@inheritDoc}
     * <p/>
     * Note that this method will force a successful closing of the current
     * thread's transaction. As such, once the index is dropped, the operation
     * is committed.
     *
     * @param indexName the name of the index to drop
     */
    public synchronized void dropIndex(final String indexName) {
        this.autoStartTransaction(true);
        if (this.rawGraph.index().existsForNodes(indexName)) {
            org.neo4j.graphdb.index.Index<Node> nodeIndex = this.rawGraph.index().forNodes(indexName);
            if (nodeIndex.isWriteable()) {
                nodeIndex.delete();
            }
        } else if (this.rawGraph.index().existsForRelationships(indexName)) {
            RelationshipIndex relationshipIndex = this.rawGraph.index().forRelationships(indexName);
            if (relationshipIndex.isWriteable()) {
                relationshipIndex.delete();
            }
        }
        this.commit();
    }

    public Iterable<Index<? extends Element>> getIndices() {
        this.autoStartTransaction(false);
        final List<Index<? extends Element>> indices = new ArrayList<Index<? extends Element>>();
        for (final String name : this.rawGraph.index().nodeIndexNames()) {
            if (!name.equals(Neo4j2Tokens.NODE_AUTO_INDEX))
                indices.add(new Neo4j2Index(name, Vertex.class, this));
        }
        for (final String name : this.rawGraph.index().relationshipIndexNames()) {
            if (!name.equals(Neo4j2Tokens.RELATIONSHIP_AUTO_INDEX))
                indices.add(new Neo4j2Index(name, Edge.class, this));
        }
        return indices;
    }

    public Neo4j2Vertex addVertex(final Object id) {
        this.autoStartTransaction(true);
        return new Neo4j2Vertex(this.rawGraph.createNode(), this);
    }

    public Neo4j2Vertex getVertex(final Object id) {
        this.autoStartTransaction(false);

        if (null == id)
            throw ExceptionFactory.vertexIdCanNotBeNull();

        try {
            final Long longId;
            if (id instanceof Long)
                longId = (Long) id;
            else if (id instanceof Number)
                longId = ((Number) id).longValue();
            else
                longId = Double.valueOf(id.toString()).longValue();
            return new Neo4j2Vertex(this.rawGraph.getNodeById(longId), this);
        } catch (NotFoundException e) {
            return null;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * {@inheritDoc}
     * <p/>
     * The underlying Neo4j graph does not natively support this method within a
     * transaction. If the graph is not currently in a transaction, then the
     * operation runs efficiently and correctly. If the graph is currently in a
     * transaction, please use setCheckElementsInTransaction() if it is
     * necessary to ensure proper transactional semantics. Note that it is
     * costly to check if an element is in the transaction.
     *
     * @return all the vertices in the graph
     */
    public Iterable<Vertex> getVertices() {
        this.autoStartTransaction(false);
        return new Neo4j2ElementIterable<Node, Vertex>(GlobalGraphOperations.at(rawGraph).getAllNodes(), this);
    }

    public Iterable<Vertex> getVertices(final String key, final Object value) {
        this.autoStartTransaction(false);
        final AutoIndexer<Node> indexer = this.rawGraph.index().getNodeAutoIndexer();
        if (indexer.isEnabled() && indexer.getAutoIndexedProperties().contains(key))
            return new Neo4j2ElementIterable<Node, Vertex>(this.rawGraph.index().getNodeAutoIndexer().getAutoIndex().get(key, value), this);
        else
            return new PropertyFilteredIterable<Vertex>(key, value, this.getVertices());
    }

    /**
     * {@inheritDoc}
     * <p/>
     * The underlying Neo4j graph does not natively support this method within a
     * transaction. If the graph is not currently in a transaction, then the
     * operation runs efficiently and correctly. If the graph is currently in a
     * transaction, please use setCheckElementsInTransaction() if it is
     * necessary to ensure proper transactional semantics. Note that it is
     * costly to check if an element is in the transaction.
     *
     * @return all the edges in the graph
     */
    public Iterable<Edge> getEdges() {
        this.autoStartTransaction(false);
        return new Neo4j2ElementIterable<Relationship, Edge>(GlobalGraphOperations.at(rawGraph).getAllRelationships(), this);
    }

    public Iterable<Edge> getEdges(final String key, final Object value) {
        this.autoStartTransaction(false);
        final AutoIndexer<Relationship> indexer = this.rawGraph.index().getRelationshipAutoIndexer();
        if (indexer.isEnabled() && indexer.getAutoIndexedProperties().contains(key))
            return new Neo4j2ElementIterable<Relationship, Edge>(this.rawGraph.index().getRelationshipAutoIndexer().getAutoIndex().get(key, value), this);
        else
            return new PropertyFilteredIterable<Edge>(key, value, this.getEdges());
    }

    
    private <T extends Element> boolean isVertexClass(Class<T> elementClass){
    	if (elementClass == null){
            throw ExceptionFactory.classForElementCannotBeNull();
    	} else if (Vertex.class.isAssignableFrom(elementClass)) {
    		return true;
    	} else if (Edge.class.isAssignableFrom(elementClass)) {
    		return false;
    	} else {
    		throw ExceptionFactory.classIsNotIndexable(elementClass);
    	}
    }
    
    
    /**
     * @return Whether {@code key} was actually indexed.
     */
    private <T extends PropertyContainer> boolean dropKeyIndex(AutoIndexer<T> indexer, String key){
    	if (indexer.isEnabled() && (indexer.getAutoIndexedProperties().contains(key))){
    		indexer.stopAutoIndexingProperty(key);
    		return true;
    	}
    	return false;
    }
    
    private <T extends Element> void dropKeyIndex(final String key, final Class<T> elementClass, boolean persist) {
    	if(isVertexClass(elementClass)){
    		if(dropKeyIndex(this.rawGraph.index().getNodeAutoIndexer(), key) && persist){
    			persistVertexAutoIndexerKeys();
    		}
    	} else {
    		if(dropKeyIndex(this.rawGraph.index().getRelationshipAutoIndexer(), key) && persist){
    			persistEdgeAutoIndexerKeys();
    		}
    	}
    }
    
    @Override
    public <T extends Element> void dropKeyIndex(final String key, final Class<T> elementClass) {
    	dropKeyIndex(key, elementClass, true);
    }

    
    private <T extends PropertyContainer> void createKeyIndex(AutoIndexer<T> indexer, boolean isVertexIndex, String key, boolean persist){
    	if (!indexer.isEnabled()){
            indexer.setEnabled(true);
        }

        if(indexer.getAutoIndexedProperties().contains(key)){
        	String.format("Nothing to do. %s property, '%s', is already auto-index.", isVertexIndex ? "Vertex" : "Edge", key);
        } else {
        	indexer.startAutoIndexingProperty(key);
        	this.autoStartTransaction(true);
        	
        	KeyIndexableGraphHelper.reIndexElements(this, isVertexIndex ? this.getVertices() : this.getEdges(), new HashSet<String>(Arrays.asList(key)));
        	
        	if(persist){
        		if(isVertexIndex){
        			persistVertexAutoIndexerKeys();
        		} else {
        			persistEdgeAutoIndexerKeys();
        		}
        	}
        }
    }
    
    
	private <T extends Element> void createKeyIndex(final String key, final Class<T> elementClass, boolean persist) {
        if (isVertexClass(elementClass)){
        	createKeyIndex(this.rawGraph.index().getNodeAutoIndexer(), true, key, persist);
        } else {
        	createKeyIndex(this.rawGraph.index().getRelationshipAutoIndexer(), false, key, persist);
        }
    }
    
    @Override
    @SuppressWarnings("rawtypes")
	public <T extends Element> void createKeyIndex(final String key, final Class<T> elementClass, final Parameter... indexParameters) {
    	createKeyIndex(key, elementClass, true);
    }

    
    private <T extends PropertyContainer> Set<String> getIndexedKeys(AutoIndexer<T> indexer) {
    	if (indexer.isEnabled()){
    		return indexer.getAutoIndexedProperties();
    	} else {
    		return Collections.emptySet();
    	}
    }
    
    @Override
    public <T extends Element> Set<String> getIndexedKeys(final Class<T> elementClass) {
    	if(isVertexClass(elementClass)){
    		return getIndexedKeys(rawGraph.index().getNodeAutoIndexer());
    	} else {
    		return getIndexedKeys(rawGraph.index().getRelationshipAutoIndexer());
    	}
    }

    public void removeVertex(final Vertex vertex) {
        this.autoStartTransaction(true);

        try {
            final Node node = ((Neo4j2Vertex) vertex).getRawVertex();
            for (final Relationship relationship : node.getRelationships(org.neo4j.graphdb.Direction.BOTH)) {
                relationship.delete();
            }
            node.delete();
        } catch (NotFoundException nfe) {
            throw ExceptionFactory.vertexWithIdDoesNotExist(vertex.getId());
        } catch (IllegalStateException ise) {
            // wrap the neo4j exception so that the message is consistent in blueprints.
            throw ExceptionFactory.vertexWithIdDoesNotExist(vertex.getId());
        }
    }

    public Neo4j2Edge addEdge(final Object id, final Vertex outVertex, final Vertex inVertex, final String label) {
        if (label == null)
            throw ExceptionFactory.edgeLabelCanNotBeNull();

        this.autoStartTransaction(true);
        return new Neo4j2Edge(((Neo4j2Vertex) outVertex).getRawVertex().createRelationshipTo(((Neo4j2Vertex) inVertex).getRawVertex(),
                DynamicRelationshipType.withName(label)), this);
    }

    public Neo4j2Edge getEdge(final Object id) {
        if (null == id)
            throw ExceptionFactory.edgeIdCanNotBeNull();

        this.autoStartTransaction(true);
        try {
            final Long longId;
            if (id instanceof Long)
                longId = (Long) id;
            else
                longId = Double.valueOf(id.toString()).longValue();
            return new Neo4j2Edge(this.rawGraph.getRelationshipById(longId), this);
        } catch (NotFoundException e) {
            return null;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public void removeEdge(final Edge edge) {
        this.autoStartTransaction(true);
        ((Relationship) ((Neo4j2Edge) edge).getRawElement()).delete();
    }

    public void stopTransaction(Conclusion conclusion) {
        if (Conclusion.SUCCESS == conclusion)
            commit();
        else
            rollback();
    }

    public void commit() {
        if (null == tx.get()) {
            return;
        }

        try {
            tx.get().success();
        } finally {
            tx.get().close();
            tx.remove();
        }
    }

    public void rollback() {
        if (null == tx.get()) {
            return;
        }

        try{
        	tx.get().failure();
        } finally {
            tx.get().close();
            tx.remove();
        }
    }

    public void shutdown() {
        try {
            this.commit();
        } catch (TransactionFailureException e) {
            logger.warning("Failure on shutdown "+e.getMessage());
            // TODO: inspect why certain transactions fail
        }
        this.rawGraph.shutdown();
    }

    // The forWrite flag is true when the autoStartTransaction method is
    // called before any operation which will modify the graph in any way. It
    // is not used in this simple implementation but is required in subclasses
    // which enforce transaction rules. Now that Neo4j reads also require a
    // transaction to be open it is otherwise impossible to tell the difference
    // between the beginning of a write operation and the beginning of a read
    // operation.
    public void autoStartTransaction(boolean forWrite) {
        if (tx.get() == null)
            tx.set(this.rawGraph.beginTx());
    }

    public GraphDatabaseService getRawGraph() {
        return this.rawGraph;
    }

    public Features getFeatures() {
        return FEATURES;
    }

    public String toString() {
        return StringFactory.graphString(this, this.rawGraph.toString());
    }

    public GraphQuery query() {
        return new DefaultGraphQuery(this);
    }

    public Iterator<Map<String,Object>> query(String query, Map<String,Object> params) {
        return cypher.execute(query,params==null ? Collections.<String,Object>emptyMap() : params).javaIterator();
    }

}
