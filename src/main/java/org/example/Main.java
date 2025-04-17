package org.example;

import java.io.IOException;

import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.example.DBFGenerator.generateDbfFile;

public class Main {

    public static void main(String[] args) {
        // Путь к вашему DBF‑файлу
        String filePath = "src/main/parcels_new_1_df2a__7829Polygon.dbf";
        // Основная кодировка для большинства строк в заголовке – cp866.
        Charset cp866 = Charset.forName("cp866");

        try (RandomAccessFile raf = new RandomAccessFile(filePath, "r")) {
            // Читаем заголовок (32 байта)
            byte[] headerBytes = new byte[32];
            raf.readFully(headerBytes);

            // Число записей хранится в байтах 4-7 (little-endian)
            int numRecords = ((headerBytes[7] & 0xFF) << 24)
                    | ((headerBytes[6] & 0xFF) << 16)
                    | ((headerBytes[5] & 0xFF) << 8)
                    | (headerBytes[4] & 0xFF);

            // Длина заголовка – байты 8-9 (little-endian)
            int headerLength = ((headerBytes[8] & 0xFF)) | ((headerBytes[9] & 0xFF) << 8);
            // Длина записи – байты 10-11 (little-endian)
            int recordLength = ((headerBytes[10] & 0xFF)) | ((headerBytes[11] & 0xFF) << 8);

            System.out.println("Number of records: " + numRecords);
            System.out.println("Header length: " + headerLength);
            System.out.println("Record length: " + recordLength);

            // Читаем описатель полей (начинается сразу после заголовка, до символа 0x0D)
            List<FieldInfo> fields = new ArrayList<>();
            // Текущая позиция уже = 32, читаем до встреченного байта 0x0D.
            while (true) {
                byte b = raf.readByte();
                if (b == 0x0D) {
                    break; // конец описателя полей
                }
                // Возвращаемся на 1 байт, чтобы прочитать целиком 32-байтный описатель поля
                raf.seek(raf.getFilePointer() - 1);
                byte[] fieldDescBytes = new byte[32];
                raf.readFully(fieldDescBytes);
                // Имя поля – первые 11 байт, обрезая пробелы
                String fieldName = new String(fieldDescBytes, 0, 11, cp866).trim();
                char fieldType = (char) fieldDescBytes[11];
                // Длина поля находится в байте с индексом 16, количество десятичных – в байте 17
                int fieldLength = fieldDescBytes[16] & 0xFF;
                int decimalCount = fieldDescBytes[17] & 0xFF;
                FieldInfo fi = new FieldInfo(fieldName, fieldType, fieldLength, decimalCount);
                fields.add(fi);
            }

            // Выводим список полей
            System.out.println("Поля:");
            for (FieldInfo field: fields) {
                System.out.print(field.name + "\t");
            }
            System.out.println();

            // Создаем переменные:
            // Список названий полей
            List<String> fieldNames = new ArrayList<>();
            for (FieldInfo field: fields) {
                fieldNames.add(field.name);
            }

            // Список записей, где каждая запись – список значений
            List<List<Object>> recordsList = new ArrayList<>();

            // Переходим к началу записей (позиция = headerLength)
            raf.seek(headerLength);

            System.out.println("Записи:");
            // Обрабатываем каждую запись
            for (int rec = 0; rec < numRecords; rec++) {
                // Позиция записи: headerLength + (индекс записи) * recordLength
                long recordPos = headerLength + rec * recordLength;
                raf.seek(recordPos);
                // Первый байт — флаг удаления (игнорируем)
                byte deletionFlag = raf.readByte();

                Object[] recordValues = new Object[fields.size()];
                // Для каждого поля читаем field.length байт
                for (int i = 0; i < fields.size(); i++) {
                    FieldInfo field = fields.get(i);
                    byte[] fieldBytes = new byte[field.length];
                    raf.readFully(fieldBytes);

                    if (field.type == 'N') {  // числовые поля
                        String numStr = new String(fieldBytes, cp866).trim();
                        if (numStr.isEmpty()) {
                            recordValues[i] = null;
                        } else {
                            try {
                                recordValues[i] = Double.parseDouble(numStr);
                            } catch (NumberFormatException e) {
                                recordValues[i] = numStr;
                            }
                        }
                    } else if (field.type == '@') {  // поля даты
                        if (fieldBytes.length >= 4) {
                            int julianDay = ((fieldBytes[0] & 0xFF))
                                    | ((fieldBytes[1] & 0xFF) << 8)
                                    | ((fieldBytes[2] & 0xFF) << 16)
                                    | ((fieldBytes[3] & 0xFF) << 24);
                            String dateStr = julianToGregorian(julianDay);
                            recordValues[i] = dateStr;
                        } else {
                            recordValues[i] = "";
                        }
                    } else {  // прочие поля (например, строковые)
                        // Если поле "agreement", то используем UTF-8, иначе – cp866
                        Charset fieldCharset = field.name.equalsIgnoreCase("agreement")
                                ? Charset.forName("UTF-8")
                                : cp866;
                        String strValue = new String(fieldBytes, fieldCharset).trim();
                        recordValues[i] = strValue;
                    }
                }
                // Вывод значений записи
                List<Object> recordRow = new ArrayList<>();
                for (Object value: recordValues) {
                    System.out.print(value + "\t");
                    recordRow.add(value);
                }
                System.out.println();
                // Добавляем запись в общий список
                recordsList.add(recordRow);
            }

            // Теперь у вас есть две переменные:
            // fieldNames – список названий полей,
            // recordsList – список записей с их значениями.
            System.out.println("Список названий полей: " + fieldNames);
            System.out.println("Список значений записей: " + recordsList);

            System.out.println(DBFGenerator.class);

            generateDbfFile(fieldNames, recordsList);
            System.out.println("DBF-файл 'output.dbf' успешно создан.");
        } catch (IOException e) {
            e.printStackTrace();
        }

        //переменные

    }

    /**
     * Преобразует юлианский день в дату по Григорианскому календарю в формате MM/dd/yyyy. Например, для julianDay =
     * 2460027 получим "03/23/2023".
     */
    public static String julianToGregorian(int julianDay) {
        int l = julianDay + 68569;
        int n = (4 * l) / 146097;
        l = l - (146097 * n + 3) / 4;
        int i = (4000 * (l + 1)) / 1461001;
        l = l - (1461 * i) / 4 + 31;
        int j = (80 * l) / 2447;
        int day = l - (2447 * j) / 80;
        l = j / 11;
        int month = j + 2 - 12 * l;
        int year = 100 * (n - 49) + i + l;
        return String.format("%02d/%02d/%04d", month, day, year);
    }

    // Класс для хранения информации о поле из DBF-файла
    static class FieldInfo {

        String name;
        char type;
        int length;
        int decimalCount;

        public FieldInfo(String name, char type, int length, int decimalCount) {
            this.name = name;
            this.type = type;
            this.length = length;
            this.decimalCount = decimalCount;
        }
    }
}
