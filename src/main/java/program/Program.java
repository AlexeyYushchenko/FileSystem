package program;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;

/**
 * Проект по теме "Работа с файловой системой на языке Java".
 * 1. Программа получает на вход имя корневой директории, где находится папка "new_data" (V)
 * 2. Программа должна сканировать сетевую папку раз в 10 секунд и проверять наличие в ней новых файлов. (V)
 * Файлы имеют название RE_FRAUD_LIST_yyyyMMdd_000000_00000.txt
 * <p>
 * Все новые файлы должны быть обработаны в следующем порядке:
 * •	на основании имеющихся файлов сгенерировать новые файлы
 * и записать их в соответствующую папку с названием ОПЕРАТОРА,
 * находящихся в папке processed_data
 * •	каждый новый файл должен иметь следующее название ОПЕРАТОР_FRAUD_LIST_yyyyMMdd_*.0.txt, где *.0 - порядковый номер файла в папке ОПЕРАТОРА
 * •	в каждый новый файл вносится информация из соответствующего ему файлу по дате, игнорируя данные, в столбцах которых указано NO_FRAUD, группируя данные по каждому оператору ОТДЕЛЬНО
 * 3. Все обработанные файлы из папки "new_data" перенести в папку "processed_data\processed". В случае, если в целевой папке уже имеются файлы с такими же названиями, то перенести файлы под новыми именами по шаблону имя_файла(номер_начиная_с_единицы).txt
 */

public class Program {

    private static final Logger logger = LogManager.getLogger(Program.class);

    public static void main(String[] args) {

        try {
            while (true) {
                checkFiles(args[0]);
                Thread.sleep(10_000);
            }
        } catch (InterruptedException e) {
            logger.debug(e.getMessage());
        } catch (IOException e) {
            logger.trace(e.getMessage());
        }
    }

    private static void checkFiles(String directory) throws IOException {

        File folder = new File(directory + "\\new data");
        folder.mkdirs();

        for (File file : folder.listFiles()) {

            if (file.isDirectory()) continue;

            if (file.getName().matches("RE_FRAUD_LIST_\\d{8}_\\d{6}_\\d{5}.txt")) {

                Map<String, List<String>> map = new HashMap<>();
                List<String> lines = Files.readAllLines(file.toPath());
                for (String line : lines) {
                    try {
                        String[] split = line.split("\\|");
                        String status = split[4];
                        if ("FRAUD".equals(status)) {
                            String operator = split[2];
                            List<String> list = map.getOrDefault(operator, new ArrayList<>());
                            String sub = line.substring(line.indexOf("|"));
                            list.add((list.size() + 1) + sub);
                            map.put(operator, list);
                        }
                    } catch (Exception e) {
                        logger.debug(e.getMessage());
                    }
                }

                String dateOfFile = file.getName().split("_")[3];

                for (var entry : map.entrySet()) {
                    String operator = entry.getKey();

                    File operatorFolder = new File(directory + "\\processed_data\\processed\\" + operator);
                    operatorFolder.mkdirs();

                    File operatorDir = new File(directory + "\\processed_data\\processed\\" + operator);

                    //Укоротить split?
//                    int ordinal = Arrays.stream(operatorDir.listFiles())
//                            .map(f -> {
//                                int length = f.toString().split("_").length;
//                                return f.toString().split("_")[length - 1].substring(0, 5);
//                            })
//                            .mapToInt(Integer::parseInt)
//                            .max()
//                            .orElse(0) + 1;

                    int ordinal = Arrays.stream(operatorDir.listFiles())
                            .map(f -> f.toString().substring(f.toString().length() - 9, f.toString().length() - 4)) //оставляем только номер перед .txt в имени файла "RE_FRAUD_LIST_yyyyMMdd_000000_00000.txt"
                            .mapToInt(Integer::parseInt)
                            .max()
                            .orElse(0) + 1;

                    File file1 = new File(operatorFolder + String.format("\\%s_FRAUD_LIST_%s_%05d.txt", operator, dateOfFile, ordinal));
                    List<String> reports = entry.getValue();
                    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file1))) {
                        writer.write("№|Дт/Вр звонка|Оператор|Номер абонента|Фрод|Дата выявл-я|Время выявления");
                        for (String report : reports) {
                            writer.write("\n" + report);
                        }
                    }
                }

                File dest = new File(directory + "\\processed_data\\processed");
                dest.mkdirs();
                file.renameTo(new File(dest + "\\" + file.getName()));
            }
        }
    }
}