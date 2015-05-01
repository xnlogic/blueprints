package com.tinkerpop.blueprints.impls.neo4j2.util;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

/**
 * Convenience class that keeps track of the thread-local versions of:
 * 
 * - Current (i.e. open) Transaction object.
 * - Boolean indicating whether the current transaction is for read or write operations.
 * - Id's of vertices that have been deleted during the current transaction. 
 * 
 * @author Joey Freund
 *
 */
public class TransactionManager{

	
	private ThreadLocal<Transaction> tx;
	private ThreadLocal<Boolean> forWrite;
	private ThreadLocal<Set<Long>> deletedVertexIds;
	
	private GraphDatabaseService rawGraph;
	
	
	public TransactionManager(final GraphDatabaseService rawGraph) {
		this.rawGraph = rawGraph;
		
		tx = new ThreadLocal<Transaction>(){
			@Override
			protected Transaction initialValue() {
				return null;
			}
		};
		
		forWrite = new ThreadLocal<Boolean>(){
			@Override
			protected Boolean initialValue() {
				return false;
			}
		};
		
		deletedVertexIds = new ThreadLocal<Set<Long>>(){
			@Override
			protected Set<Long> initialValue() {
				return new HashSet<Long>();
			}
		};
	}
	
	
	
	public void addDeletedVertexId(long vertexId){
		deletedVertexIds.get().add(vertexId);
	}
	
	
	public boolean containsDeletedVertexId(long vertexId){
		return deletedVertexIds.get().contains(vertexId);
	}
	
	
	private void clearThreadLocals(){
		tx.remove();
		forWrite.remove();
		deletedVertexIds.remove();
	}
	
	
	
	private void commitOrRollback(boolean commit) {
        if (null == tx.get()) {
            return;
        }

        try {
        	
        	if(forWrite.get().booleanValue()){
        		if(commit){
        			tx.get().success();
        		} else {
        			tx.get().failure();
        		}
        	}
        	
        } finally {
        	tx.get().close();
            clearThreadLocals();
        }
    }
	
	
    public void commit() {
        commitOrRollback(true);
    }

    public void rollback() {
        commitOrRollback(false);
    }

    
    public void autoStartTransaction(boolean forWrite) {
        if (tx.get() == null){
            this.tx.set(this.rawGraph.beginTx());
            deletedVertexIds.remove();
        }
        this.forWrite.set(this.forWrite.get() || forWrite);
    }



	public boolean isInTransaction() {
		return tx.get() != null;
	}
	
	
	public void shutdown(){
		commit();
		clearThreadLocals();
	}
	
}
