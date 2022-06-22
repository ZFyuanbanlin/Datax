package com.alibaba.datax.plugin.writer.hdfswriter;

import com.alibaba.datax.common.spi.ErrorCode;
import java.io.PrintWriter;
import java.io.StringWriter;

public class ZFDataxException
        extends RuntimeException
{

    private static final long serialVersionUID = 1L;

    private final transient ErrorCode errorCode;

    public ZFDataxException(ErrorCode errorCode, String errorMessage)
    {
        super(errorCode.toString() + " - " + errorMessage);
        this.errorCode = errorCode;
    }

    private ZFDataxException(ErrorCode errorCode, String errorMessage, Throwable cause)
    {
        super(errorCode.toString() + " - " + getMessage(errorMessage) + " - " + getMessage(cause), cause);

        this.errorCode = errorCode;
    }

    public static ZFDataxException asZFDataxException(ErrorCode errorCode, String message)
    {
        return new ZFDataxException(errorCode, message);
    }

    public static ZFDataxException asZFDataxException(ErrorCode errorCode, String message, Throwable cause)
    {
        if (cause instanceof ZFDataxException) {
            return (ZFDataxException) cause;
        }
        return new ZFDataxException(errorCode, message, cause);
    }

    public static ZFDataxException asZFDataxException(ErrorCode errorCode, Throwable cause)
    {
        if (cause instanceof ZFDataxException) {
            return (ZFDataxException) cause;
        }
        return new ZFDataxException(errorCode, getMessage(cause), cause);
    }

    private static String getMessage(Object obj)
    {
        if (obj == null) {
            return "";
        }

        if (obj instanceof Throwable) {
            StringWriter str = new StringWriter();
            PrintWriter pw = new PrintWriter(str);
            ((Throwable) obj).printStackTrace(pw);
            return str.toString();
        }
        else {
            return obj.toString();
        }
    }

    public ErrorCode getErrorCode()
    {
        return this.errorCode;
    }
}
