package ru.effective_mobile.model;

import lombok.Data;

@Data
public class Task {

    private final int id;
    private final String fileName;
    private final int countReduceTask;
}