package atscale.api;

/**
 * APIConstants class will be used for defining all the constants related to API.
 */
public class APIConstants {

    /**
     *
     */
    private APIConstants() {
        throw new IllegalStateException("APIConstants Utility class");
    }

    public static final String HTTPS = "https";
    public static final String HTTP = "http";

    public static final String AUTHORIZATION =  "Authorization";
    public static final String BEARER =  "Bearer ";
    public static final String BASIC =  "Basic ";
    public static final String CONTENT_TYPE = "Content-Type";
    public static final String APPLICATION_XML = "application/xml";
    public static final String FORWARD_SLASH = "/";
    public static final String ENGINE = "engine";

}
