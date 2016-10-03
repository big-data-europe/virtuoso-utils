package eu.bde.virtuoso.utils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import javax.xml.datatype.DatatypeConfigurationException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Before;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.Rio;
import org.slf4j.LoggerFactory;

/**
 *
 * @author turnguard
 */
public class VirtuosoStreamingUploaderTest {
    
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(VirtuosoStreamingUploaderTest.class);

    static final String VIRTUOSO_HOST_KEY = "virtuoso.host";
    static final String VIRTUOSO_USER_KEY = "virtuoso.user";
    static final String VIRTUOSO_PASS_KEY = "virtuoso.pass";
    static final String VIRTUOSO_DEFAULT_GRAPH_KEY = "virtuoso.default.graph";

    static String VIRTUOSO_DEFAULT_HOST = "http://localhost:8890/sparql-graph-crud-auth";
    static String VIRTUOSO_DEFAULT_USER = "dba";
    static String VIRTUOSO_DEFAULT_PASS = "dba";
    static String VIRTUOSO_DEFAULT_GRAPH = "http://junit.org";
    
    
    public VirtuosoStreamingUploaderTest() {
    }

    @BeforeClass
    public static void setUpClass() throws DatatypeConfigurationException {
        VIRTUOSO_DEFAULT_HOST = System.getProperty(VIRTUOSO_HOST_KEY) != null ? System.getProperty(VIRTUOSO_HOST_KEY) : VIRTUOSO_DEFAULT_HOST;
        VIRTUOSO_DEFAULT_USER = System.getProperty(VIRTUOSO_USER_KEY) != null ? System.getProperty(VIRTUOSO_USER_KEY) : VIRTUOSO_DEFAULT_USER;
        VIRTUOSO_DEFAULT_PASS = System.getProperty(VIRTUOSO_PASS_KEY) != null ? System.getProperty(VIRTUOSO_PASS_KEY) : VIRTUOSO_DEFAULT_PASS;        
        VIRTUOSO_DEFAULT_GRAPH = System.getProperty(VIRTUOSO_DEFAULT_GRAPH_KEY) != null ? System.getProperty(VIRTUOSO_DEFAULT_GRAPH_KEY) : VIRTUOSO_DEFAULT_GRAPH;
    }

    @AfterClass
    public static void tearDownClass() throws RepositoryException {
    }

    @Before
    public void checkVirtuosoOnline() {
        //org.junit.Assume.assumeTrue(virtuosoOnline);
    }
    
    //@Test
    public void testInsertListOfStatements() throws MalformedURLException, RDFHandlerException{
        VirtuosoInserter inserter = new VirtuosoInserter(
                new URL(VIRTUOSO_DEFAULT_HOST), 
                new URIImpl(VIRTUOSO_DEFAULT_GRAPH), 
                VIRTUOSO_DEFAULT_USER, 
                VIRTUOSO_DEFAULT_PASS
        );
        
        List<Statement> states = new ArrayList<>();
                        states.add(new StatementImpl(new URIImpl("urn:s"), new URIImpl("urn:p"), new URIImpl("urn:o")));
                        states.add(new StatementImpl(new URIImpl("urn:s1"), new URIImpl("urn:p"), new URIImpl("urn:o")));
                        
                        inserter.startRDF();
                        for(Statement state : states){
                            inserter.handleStatement(state);
                        }
                        inserter.endRDF();
    }
       
    //@Test 
    public void testInsertFromFile() throws IOException, RDFParseException, RDFHandlerException{
        InputStream inStream = null;
        RDFParser parser = Rio.createParser(RDFFormat.TURTLE);
        RDFHandler handler = new VirtuosoInserter(
                new URL(VIRTUOSO_DEFAULT_HOST), 
                new URIImpl(VIRTUOSO_DEFAULT_GRAPH), 
                VIRTUOSO_DEFAULT_USER, 
                VIRTUOSO_DEFAULT_PASS
        );
        parser.setRDFHandler(handler);
        try {
            inStream = this.getClass().getResourceAsStream("/sample.ttl");            
            parser.parse(inStream, "");
        } finally {
            if(inStream!=null){
                inStream.close();
            }
        }
    }
    
    @Test
    public void voidTest(){}
}
