package eu.bde.virtuoso.utils;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import org.apache.commons.codec.binary.Base64;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.helpers.RDFHandlerBase;
import org.openrdf.rio.rdfxml.RDFXMLWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author http://www.turnguard.com/turnguard
 * W3C GraphStore Protocol
 * @see https://www.w3.org/TR/sparql11-http-rdf-update/ 
 * OpenLink Implementation + CURL samples
 * @see http://virtuoso.openlinksw.com/dataspace/doc/dav/wiki/Main/VirtGraphUpdateProtocolUI
 * @see http://virtuoso.openlinksw.com/dataspace/doc/dav/wiki/Main/VirtGraphProtocolCURLExamples
 */
public class VirtuosoInserter extends RDFHandlerBase {
    
    private static final Logger log = LoggerFactory.getLogger(VirtuosoInserter.class);
    private final String authorization;   
    /* e.g.: http://localhost:8890/sparql-graph-crud-auth */
    private final URL virtuosoUpdateEndpoint;
    
    private RDFXMLWriter base;
    private HttpURLConnection urlCon = null;
    private OutputStream urlOutStream = null;
    private OutputStreamWriter urlWriter = null;
    private int numStreamedStatements = 0;
    
    public VirtuosoInserter(URL virtuosoUpdateEndpoint, URI targetGraph, String user, String pass) throws MalformedURLException {
        
        this.authorization = Base64.encodeBase64String((user.concat(":").concat(pass)).getBytes());
        
        this.virtuosoUpdateEndpoint = new URL(
            virtuosoUpdateEndpoint.toString()
                .concat("?graph-uri=")
                .concat(targetGraph.stringValue())
        );        
    }

    
    @Override
    public void startRDF() throws RDFHandlerException {
        try {
            HttpURLConnection.setFollowRedirects(true);
            this.urlCon = (HttpURLConnection)virtuosoUpdateEndpoint.openConnection();            
            this.urlCon.setRequestProperty("Connection","Keep-Alive");
            this.urlCon.setRequestProperty("User-Agent", "Mozilla/4.0" );            
            this.urlCon.addRequestProperty("Expect", "100-continue");
            this.urlCon.setRequestProperty("Authorization", "Basic "+this.authorization);
            this.urlCon.addRequestProperty("Accept", "*/*");
            this.urlCon.setRequestProperty("content-type", "application/rdf+xml" );            
            this.urlCon.setDoOutput(true);
            this.urlCon.setDoInput(true);            
            this.urlCon.setRequestMethod("POST"); 
            this.urlCon.setUseCaches(false);
            this.urlCon.connect();
            this.urlOutStream = this.urlCon.getOutputStream();
            this.urlWriter = new OutputStreamWriter(this.urlOutStream, Charset.forName("UTF-8"));
            this.base = new RDFXMLWriter(this.urlWriter);            
            this.base.startRDF();
        } catch (IOException ex) {
            if(this.urlCon!=null){
                this.urlCon.disconnect();
            }
            throw new RDFHandlerException(ex);
        }       
    }

    @Override
    public void handleStatement(Statement st) throws RDFHandlerException { 
        try {
            this.base.handleStatement(st);
            this.numStreamedStatements++;
            if(this.numStreamedStatements%10000==0){
                log.debug("Streamed: " + this.numStreamedStatements + " statements");                
            }
        } catch(RDFHandlerException e){
            this.urlCon.disconnect();
            throw e;
        }
    }
    
    @Override
    public void endRDF() throws RDFHandlerException {
        try {
            this.base.endRDF();             
            try {                
                int rC = this.urlCon.getResponseCode();
                if(rC!=200 && rC!=201){
                    throw new RDFHandlerException("Couldn't upload RDF payload:"+rC);
                } 
                log.info("Streamed: " + this.numStreamedStatements + " statements");                
            } catch (IOException ex) {
                throw new RDFHandlerException(ex);
            }
        } finally {
            this.urlCon.disconnect();
        }        
    }
}
