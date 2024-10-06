package ru.effective_mobile.service;

import ru.effective_mobile.exception.ClassInstantiationException;
import ru.effective_mobile.exception.FileHandlerException;
import ru.effective_mobile.model.KeyValue;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public final class FileHandler {

    public static final Logger log = Logger.getLogger(FileHandler.class.getName());
    public static final String TEMP_FILE_FORMAT = "mr-%d-%d.txt";
    private static final String RESULT_FILE_NAME = "result.txt";

    private FileHandler() {
        throw new ClassInstantiationException();
    }

    /**
     * Читает содержимое файла и возвращает его в виде строки.
     *
     * @param fileName имя файла для чтения
     * @return содержимое файла в виде строки
     */
    public static String readFile(String fileName) {
        log.info("Reading file: " + fileName + "\n");
        StringBuilder content = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            String line;
            while ((line = reader.readLine()) != null) {
                content.append(line).append(" ");
            }
            log.info("Successfully read file: " + fileName + "\n");
        } catch (FileNotFoundException e) {
            log.warning("File not found: " + fileName + "\n");
            throw new FileHandlerException("File not found: " + fileName, e);
        } catch (IOException e) {
            log.warning("Error reading file: " + fileName + "\n");
            throw new FileHandlerException("Error reading file: " + fileName, e);
        }
        return content.toString().trim();
    }

    /**
     * Записывает промежуточные результаты map-задач в файлы.
     *
     * @param taskId уникальный идентификатор задачи
     * @param map    карта ключей и значений
     */
    public static void writeTempFile(int taskId, Map<Integer, List<KeyValue>> map) {
        log.info("Writing temp files for task ID: " + taskId + "\n");
        for (Map.Entry<Integer, List<KeyValue>> entry : map.entrySet()) {
            int reduceId = entry.getKey();
            String fileName = String.format(TEMP_FILE_FORMAT, taskId, reduceId);
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
                for (KeyValue element : entry.getValue()) {
                    writer.write(element.getKey() + " " + element.getValue());
                    writer.newLine();
                }
                log.info("Successfully wrote temp file: " + fileName + "\n");
            } catch (IOException e) {
                log.warning("Error writing to temp file: " + fileName + "\n");
                throw new FileHandlerException("Error writing to temp file: " + fileName, e);
            }
        }
    }

    /**
     * Читает промежуточный файл и возвращает список KeyValue объектов.
     *
     * @param fileName имя промежуточного файла
     * @return список ключ-значение из промежуточного файла
     */
    public static List<KeyValue> readTempFile(String fileName) {
        log.info("Reading temp file: " + fileName + "\n");
        List<KeyValue> list = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] arr = line.split(" ");
                if (arr.length == 2) {
                    list.add(new KeyValue(arr[0], arr[1]));
                } else {
                    log.warning("Invalid line format in temp file: " + fileName + " Line: " + line + "\n");
                }
            }
            log.info("Successfully read temp file: " + fileName + "\n");
        } catch (FileNotFoundException e) {
            log.warning("Temp file not found: " + fileName + "\n");
            throw new FileHandlerException("Temp file not found: " + fileName, e);
        } catch (IOException e) {
            log.warning("Error reading temp file: " + fileName + "\n");
            throw new FileHandlerException("Error reading temp file: " + fileName, e);
        }
        return list;
    }

    /**
     * Записывает результат reduce-задачи в результирующий файл.
     *
     * @param key    ключ
     * @param result итоговое значение
     */
    public static void writeResultFile(String key, String result) {
        log.info("Writing result to file: " + RESULT_FILE_NAME + "\n");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(RESULT_FILE_NAME))) {
            writer.write(key + " " + result);
            writer.newLine();
        } catch (IOException e) {
            log.warning("Error writing result to file: " + RESULT_FILE_NAME + "\n");
            throw new FileHandlerException("Error writing result to file: " + RESULT_FILE_NAME, e);
        }
    }
}