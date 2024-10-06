package ru.effective_mobile.exception;

public class FileHandlerException extends RuntimeException {
    public FileHandlerException(String message, Throwable cause) {
        super(message, cause);
    }
}