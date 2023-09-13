package atscale.biconnector.exception;

/**
 * AtScaleServerClientException can be thrown during the AtScaleServerClient
 * operation
 *
 * @author tanmay
 */
public class AtScaleServerClientException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public AtScaleServerClientException(String s, Exception ex) {
        super(s, ex);
    }
}
