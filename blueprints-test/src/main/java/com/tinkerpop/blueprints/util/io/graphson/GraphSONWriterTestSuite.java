package com.tinkerpop.blueprints.util.io.graphson;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.TestSuite;
import com.tinkerpop.blueprints.impls.GraphTest;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphSONWriterTestSuite extends TestSuite {
	
	
    public GraphSONWriterTestSuite() {
    }

    public GraphSONWriterTestSuite(GraphTest graphTest) {
        super(graphTest);
    }

    
    
    /**
     * Deprecated during the port to Java 8 - We are not supporting normalized JSON output.
     * 
     * In fact, normalized output wasn't really working correctly before:
     * The unit tests depended on Java's implementation of HashMap, specifically, the order of
     * iteration (which is NOT a part of the interface). 
     * With the move to Java 8, the implementation of HashMap changed, and the "normalized" output
     * ended up being different.
     */
    @Deprecated
    public void testGratefulGraphNormalized() throws Exception {
        Graph graph = this.graphTest.generateGraph();
        if (graph.getFeatures().supportsEdgeIteration && !graph.getFeatures().ignoresSuppliedIds) {
            this.stopWatch();
            final String readGraphSON = readFile(GraphSONReader.class.getResourceAsStream("graph-example-2-normalized.json"), Charset.forName("UTF-8"));
            new GraphSONReader(graph).inputGraph(GraphSONReader.class.getResourceAsStream("graph-example-2-normalized.json"));

            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            new GraphSONWriter(graph).outputGraph(baos, null, null, GraphSONMode.NORMAL, true);
            final String writtenGraphSON = new String(baos.toByteArray());

//            assertEquals(readGraphSON, writtenGraphSON);  // See method docstring
        }
        graph.shutdown();
    }

    

    static String readFile(final InputStream inputStream, final Charset encoding) throws IOException {
        byte[] encoded = toByteArray(inputStream);
        return encoding.decode(ByteBuffer.wrap(encoded)).toString();
    }

    static byte[] toByteArray(final InputStream is) throws IOException{
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        int reads = is.read();
        while(reads != -1){
            baos.write(reads);
            reads = is.read();
        }

        return baos.toByteArray();
    }
}
