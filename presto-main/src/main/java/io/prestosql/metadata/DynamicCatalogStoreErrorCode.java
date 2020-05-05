package io.prestosql.metadata;

import io.prestosql.spi.ErrorCode;
import io.prestosql.spi.ErrorCodeSupplier;
import io.prestosql.spi.ErrorType;

import static io.prestosql.spi.ErrorType.INTERNAL_ERROR;

public enum DynamicCatalogStoreErrorCode
        implements ErrorCodeSupplier
{
    /**
     * A data connection request failed.
     */
    DATA_CONNECTION_REQUEST_FAILED(0, INTERNAL_ERROR);

    private final ErrorCode errorCode;

    DynamicCatalogStoreErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0101_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
