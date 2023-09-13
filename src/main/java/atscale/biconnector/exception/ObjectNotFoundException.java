package atscale.biconnector.exception;

/**
 * HttpConnectionException can be thrown during the HTTP operation
 *
 * @author tanmay
 */
public class ObjectNotFoundException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    /**
     * @param message the message will be shown with the exception.
     */
    public ObjectNotFoundException(String message) {
        super(message);
    }
}
