package be.ugent.idlab.knows.mappingweaver.exceptions;

/**
 * An exception to be thrown when a mapping error is expected
 */
public class MappingException extends IllegalStateException {
    public MappingException(String message) {
        super(message);
    }

    public MappingException(Throwable cause) {
        super(cause);
    }
}
