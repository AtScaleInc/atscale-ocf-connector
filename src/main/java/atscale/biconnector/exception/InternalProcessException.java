package atscale.biconnector.exception;

/**
 * HttpConnectionException can be thrown during the HTTP operation
 *
 * @author tanmay
 */
public class InternalProcessException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    /**
     * @param message the message will be shown with the exception.
     */
    public InternalProcessException(String message) {
        super(message);
    }

    public InternalProcessException(Throwable th) {
        super(th);
    }
}
