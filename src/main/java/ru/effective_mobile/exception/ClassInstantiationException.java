package ru.effective_mobile.exception;

public class ClassInstantiationException extends RuntimeException {
    public ClassInstantiationException() {
        super("It is not possible to create an instance of this class");
    }
}