package org.example;


import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DBFGenerator {

    // Вспомогательный класс для хранения информации по полю
    private static class Field {

        String name;
        char type;       // 'C' - строка, 'N' - число, 'D' - дата
        int length;
        int decimalCount;

        Field(String name, char type, int length, int decimalCount) {
            this.name = name;
            this.type = type;
            this.length = length;
            this.decimalCount = decimalCount;
        }
    }

    /**
     * Генерирует DBF-файл (dBase III) с именем output.dbf.
     *
     * @param fieldNames  список названий полей
     * @param recordsList список записей, каждая запись – список объектов (значения по колонкам)
     *
     * @throws IOException в случае ошибок записи или обработки данных
     */
    public static void generateDbfFile(List<String> fieldNames, List<Character> fieldTypes, List<List<Object>> recordsList) throws IOException {
        // Определяем свойства для каждого поля: тип, длину и количество десятичных знаков.
        int fieldCount = fieldNames.size();
        List<Field> fields = new ArrayList<>();

        for (int col = 0; col < fieldCount; col++) {
            // Используем тип поля из исходного файла, но преобразуем '@' в 'D' для корректной записи дат
            char type = fieldTypes.get(col) == '@' ? 'D' : fieldTypes.get(col);
            int maxLength = fieldNames.get(col).length();
            int decimalCount = 0; // для числовых значений

            // Определяем максимальную длину и десятичные знаки для поля
            for (List<Object> record: recordsList) {
                if (col >= record.size()) {
                    continue;
                }
                Object value = record.get(col);
                if (value == null) {
                    continue;
                }

                if (type == 'N' && value instanceof Number) {
                    String strVal = value.toString();
                    int dotIndex = strVal.indexOf('.');
                    if (dotIndex >= 0) {
                        decimalCount = Math.max(decimalCount, strVal.length() - dotIndex - 1);
                    }
                    maxLength = Math.max(maxLength, strVal.length());
                } else if (type == 'D') {
                    maxLength = 8; // даты в формате YYYYMMDD - 8 байт
                    break;
                } else if (type == 'C') {
                    String s = value.toString();
                    maxLength = Math.max(maxLength, s.length());
                }
            }

            // При строковом поле длина должна быть не менее 1
            if (type == 'C') {
                maxLength = Math.max(maxLength, 1);
            }
            // Для числовых полей можно задать минимальную длину, например, 10 символов
            if (type == 'N') {
                maxLength = Math.max(maxLength, 10);
            }
            fields.add(new Field(fieldNames.get(col), type, maxLength, decimalCount));
        }

        // Создаем буфер для формирования бинарного файла
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        // ========================
        // Формирование заголовка файла
        // ========================
        // 1. Первый байт: версия DBF - 0x03 для dBase III без memo-файла.
        dos.writeByte(0x03);

        // 2. Следующие 3 байта: дата последнего обновления (год, месяц, день)
        Calendar cal = Calendar.getInstance();
        dos.writeByte(cal.get(Calendar.YEAR) - 1900); // год записываем как смещение от 1900
        dos.writeByte(cal.get(Calendar.MONTH) + 1);     // месяц (от 1 до 12)
        dos.writeByte(cal.get(Calendar.DAY_OF_MONTH));

        // 3. Количество записей (4 байта, little-endian)
        int numRecords = recordsList.size();
        dos.writeInt(Integer.reverseBytes(numRecords));

        // 4. Длина заголовка (2 байта, little-endian): 32 байта базового заголовка + 32 байта на поле * количество полей + 1 байт терминатора
        int headerLength = 32 + (32 * fields.size()) + 1;
        dos.writeShort(Short.reverseBytes((short) headerLength));

        // 5. Длина записи (2 байта, little-endian): 1 байт (флаг удаления) + суммарная длина всех полей
        int recordLength = 1;
        for (Field f: fields) {
            recordLength += f.length;
        }
        dos.writeShort(Short.reverseBytes((short) recordLength));

        // 6. Резерв: 20 байт (устанавливаем в 0)
        for (int i = 0; i < 20; i++) {
            dos.writeByte(0);
        }

        // ========================
        // Поле дескрипторов (каждое поле – 32 байта)
        // ========================
        for (Field f: fields) {
            // Поле имени: 11 байт. Если имя короче – дополняем нулевыми байтами.
            byte[] nameBytes = new byte[11];
            byte[] fieldNameBytes = f.name.getBytes("ASCII");
            int len = Math.min(fieldNameBytes.length, 11);
            System.arraycopy(fieldNameBytes, 0, nameBytes, 0, len);
            dos.write(nameBytes);

            // 1 байт: тип поля ('C', 'N', 'D')
            dos.writeByte((byte) f.type);

            // 4 байта: адрес данных поля (не используется, заполняем нулями)
            for (int i = 0; i < 4; i++) {
                dos.writeByte(0);
            }

            // 1 байт: длина поля
            dos.writeByte((byte) f.length);

            // 1 байт: количество десятичных знаков
            dos.writeByte((byte) f.decimalCount);

            // 14 байт: резерв (заполняем нулями)
            for (int i = 0; i < 14; i++) {
                dos.writeByte(0);
            }
        }

        // Терминатор дескрипторов поля: 0x0D
        dos.writeByte(0x0D);

        // ========================
        // Запись данных (каждая запись начинается с 1 байта – флага удаления)
        // ========================
        for (List<Object> record: recordsList) {
            // Записываем флаг удаления: 0x20 (пробел) означает, что запись не удалена.
            dos.writeByte(0x20);

            // Для каждого поля записываем данные в виде строки фиксированной длины
            for (int col = 0; col < fields.size(); col++) {
                Field f = fields.get(col);
                String fieldData = "";
                if (col < record.size() && record.get(col) != null) {
                    Object value = record.get(col);
                    if (f.type == 'N') {
                        // Для числового поля форматируем число с заданным количеством десятичных знаков.
                        double num;
                        if (value instanceof Number) {
                            num = ((Number) value).doubleValue();
                        } else {
                            try {
                                num = Double.parseDouble(value.toString());
                            } catch (NumberFormatException ex) {
                                num = 0;
                            }
                        }
                        // Формат: например, "%10.2f"
                        String format = "%" + f.length + "." + f.decimalCount + "f";
                        fieldData = String.format(Locale.US, format, num);
                    } else if (f.type == 'D') {
                        // Для поля даты форматируем значение как "YYYYMMDD"
                        if (value != null && value instanceof String) {
                            String dateStr = (String) value;
                            try {
                                // Пытаемся распознать дату в формате "MM/dd/yyyy"
                                SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
                                sdf.setLenient(false);
                                Date date = sdf.parse(dateStr);
                                
                                Calendar dateCal = Calendar.getInstance();
                                dateCal.setTime(date);
                                int year = dateCal.get(Calendar.YEAR);
                                int month = dateCal.get(Calendar.MONTH) + 1;
                                int day = dateCal.get(Calendar.DAY_OF_MONTH);
                                fieldData = String.format("%04d%02d%02d", year, month, day);
                            } catch (ParseException e) {
                                // Если не удалось распарсить дату, заполняем пробелами
                                fieldData = "        "; // 8 пробелов
                            }
                        } else {
                            // Если значение null, заполняем пробелами
                            fieldData = "        "; // 8 пробелов
                        }
                    } else {
                        // Для строкового поля – просто преобразуем значение в строку.
                        fieldData = value.toString();
                        if (fieldData.length() > f.length) {
                            fieldData = fieldData.substring(0, f.length);
                        }
                    }
                }
                // Если значение короче требуемой длины, дополняем пробелами:
                if (f.type == 'C' || f.type == 'D') {
                    fieldData = String.format("%-" + f.length + "s", fieldData);
                } else if (f.type == 'N') {
                    fieldData = String.format("%" + f.length + "s", fieldData);
                }
                // Пишем данные поля как последовательность байтов (ASCII)
                dos.writeBytes(fieldData);
            }
        }

        // Файл завершается символом конца файла 0x1A.
        dos.writeByte(0x1A);
        dos.flush();
        byte[] dbfContent = baos.toByteArray();
        dos.close();

        // Записываем сформированный контент в файл output.dbf
        try (FileOutputStream fos = new FileOutputStream("output.dbf")) {
            fos.write(dbfContent);
        }
    }
}