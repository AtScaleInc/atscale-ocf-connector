package atscale.api;

import atscale.biconnector.configuration.AtScaleBIConfiguration;
import atscale.biconnector.exception.HttpConnectionException;
import atscale.biconnector.utils.SOAPResultSet;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import org.apache.http.client.HttpClient;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static atscale.api.APIConstants.*;
import static atscale.biconnector.utils.Constants.*;

/**
 * SOAPQuery Class
 */
public class SOAPQuery {
    private static final Logger LOGGER = Logger.getLogger(SOAPQuery.class);

    private SOAPQuery() {
        throw new IllegalStateException("SOAPQuery Utility class");
    }

    public static SOAPResultSet runSOAPQuery(AtScaleServerClient atScaleServerClient,
                                             AtScaleBIConfiguration configuration, String query) {
        return runSOAPQuery(atScaleServerClient, configuration, query, null);
    }

    public static SOAPResultSet runSOAPQuery(AtScaleServerClient atScaleServerClient,
                                             AtScaleBIConfiguration configuration, String query, String extraProperty) {
        SOAPResultSet resultSet = new SOAPResultSet();
        try {
            String body = BEFORE_QUERY + query;

            // Check & see if we have to add an extra property to the SOAP call
            if (extraProperty != null) {
                String newAfterQuery = AFTER_QUERY;
                newAfterQuery = newAfterQuery.replace("<PropertyList>\n", "<PropertyList>\n" + extraProperty);
                body += newAfterQuery;
            } else {
                body += AFTER_CATALOG_QUERY; // this would be AFTER_QUERY
            }

            atScaleServerClient.connect();
            String token = atScaleServerClient.getConnection();
            String url = atScaleServerClient.getUrlQuery();

            HttpClient httpClient = configuration.getHttpClient();
            Unirest.setHttpClient(httpClient);

            HttpResponse<String> response = Unirest.post(url).header(AUTHORIZATION, BEARER + token)
                    .header(CONTENT_TYPE, APPLICATION_XML).body(body).asString();

            if (response.getBody().contains("Schema not found")) {
                LOGGER.error("DMV call failed with 'Schema not found'. Make sure project is published");
            } else if (!response.getBody().toLowerCase(Locale.ROOT).contains("envelope")) {
                throw new HttpConnectionException("DMV call failed with: " + response.getBody());
            } else {
                if (processSOAPResult(resultSet, response)) return resultSet;
            }
            return resultSet;
        } catch (Exception ex) {
            LOGGER.error("Error while running SOAP query: " + ex);
            return null;
        }
    }

    /**
     * @param resultSet
     * @param request
     * @return
     */
    private static boolean processSOAPResult(SOAPResultSet resultSet, HttpResponse<String> request) {
        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            dbf.setNamespaceAware(false);
            DocumentBuilder db = dbf.newDocumentBuilder();
            InputSource is = new InputSource(new StringReader(request.getBody()));
            Document doc = db.parse(is);

            // Check for a SOAP fault to return error
            XPathExpression expr = XPathFactory.newInstance().newXPath()
                    .compile("/Envelope/Body/Fault/faultstring");
            Object obj = expr.evaluate(doc, XPathConstants.NODESET);
            if (isNodeList(obj)) {
                if (((NodeList) obj).item(0).getFirstChild().getTextContent()
                        .equals("next on empty iterator")) {
                    LOGGER.warn("Empty results returned from SOAP query");
                    return true;
                }
                throw new HttpConnectionException("SOAP Fault: " + ((NodeList) obj).item(0).getFirstChild().getTextContent()); // +rmSoapErrTag(((NodeList)
            }

            // Find query ID and log it
            expr = XPathFactory.newInstance().newXPath().compile("/Envelope/Header/queryId");
            obj = expr.evaluate(doc, XPathConstants.NODESET);
            if (isNodeList(obj)) {
                LOGGER.info("queryID: " + ((NodeList) obj).item(0).getFirstChild().getTextContent());
            }

            // Let's get the list of Fields (fields metadata)
            XPathExpression exprMetadata = XPathFactory.newInstance().newXPath()
                    .compile("/Envelope/Body/ExecuteResponse/return/root/schema/complexType/sequence/element");

            // Evaluate expression result on XML document
            Object hitsMetadata = exprMetadata.evaluate(doc, XPathConstants.NODESET);
            if (hitsMetadata instanceof NodeList) {
                NodeList list = (NodeList) hitsMetadata;
                for (int i = 0; i < list.getLength(); i++) {
                    Node node = list.item(i);
                    NamedNodeMap attrs = node.getAttributes();
                    Node nameNode = attrs.getNamedItem("name");
                    Node typeNode = attrs.getNamedItem("type");
                    resultSet.insertColumn(nameNode.getTextContent(), typeNode.getTextContent());
                }
            }

            // Now let's get read the data & create the ResultSet
            //
            XPathExpression exprData = XPathFactory.newInstance().newXPath()
                    .compile("/Envelope/Body/ExecuteResponse/return/root/row");

            // Evaluate expression result on XML document
            Object hitsData = exprData.evaluate(doc, XPathConstants.NODESET);
            if (hitsData instanceof NodeList) {
                NodeList list = (NodeList) hitsData;

                // Move through each row
                for (int i = 0; i < list.getLength(); i++) {
                    Node nodeRow = list.item(i);
                    NodeList children = nodeRow.getChildNodes();

                    // Add each column value to an array
                    List<String> values = new ArrayList<>();
                    for (int j = 0; j < children.getLength(); j++) {
                        Node nodeData = children.item(j);
                        values.add(nodeData.getTextContent());
                    }
                    resultSet.insertRow(values);
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error while extracting the result from SOAP query: ", e);
        }
        return false;
    }

    private static boolean isNodeList(Object obj) {
        return obj instanceof NodeList && ((NodeList) obj).getLength() > 0;
    }
}
