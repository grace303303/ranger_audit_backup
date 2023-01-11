package org.example;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.LocalDate;
import java.util.StringJoiner;
import java.util.stream.Stream;


public class Utilities {

    public static Namespace getUserInputs(String[] args) {

        ArgumentParser parser = ArgumentParsers.newFor("ranger-audits").build()
                .defaultHelp(true)
                .description("Download ranger audits from Cloud and upload them into Solr.");
        parser.addArgument("--cloud_type")
                .choices("aws", "azure", "AWS", "AZURE", "Aws", "Azure")
                .required(true)
                .help("The cloud type, it should be either AWS or AZURE.");
        parser.addArgument("--storage_location")
                .required(true)
                .help("The storage location where the data is stored, without the prefix. (example: my-bucket-name/my-env-name/data)");
        parser.addArgument("--solr_path")
                .required(true)
                .help("The Solr path where we want to insert the content into. (example: my-env0.myname.xcu2-8y8x.wl.cloudera.site:8985)");
        parser.addArgument("--days_ago")
                .type(Integer.class)
                .required(true)
                .help("How many days ago we want to start downloading the logs.");
        parser.addArgument("--access_key_id")
                .required(true)
                .help("Cloud Access Key ID.");
        parser.addArgument("--secret_access_key")
                .help("AWS Secret Access Key.");

        Namespace res = null;

        try {
            res = parser.parseArgs(args);
            System.out.println(res);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }

        return res;

    }

    public static LocalDate getDaysAgoDate(int daysAgo) {
        LocalDate todayDate = LocalDate.now();
        return todayDate.minusDays(daysAgo);

    }

    public static boolean isDateStr(String potentialDate) {
        DateTimeFormatter dateFormatter = DateTimeFormatter.BASIC_ISO_DATE;

        try {
            LocalDate.parse(potentialDate, dateFormatter);
        } catch (DateTimeParseException e) {
            return false;
        }

        return true;
    }

    public static boolean isLaterDate(String potentialDate, int daysAgo) {

        if (!isDateStr(potentialDate)) {
            return false;
        }

        //convert String, for example "20230112", to LocalDate.
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        LocalDate newPotentialDate = LocalDate.parse(potentialDate, formatter);

        LocalDate dateDaysAgo = getDaysAgoDate(daysAgo);

        return newPotentialDate.equals(dateDaysAgo) || newPotentialDate.isAfter(dateDaysAgo);

    }

    public static String readFileAsJsonList(String pathStr) throws ParseException {
        Path filePath = Paths.get(pathStr);
        StringBuilder contentBuilder = new StringBuilder();
        JSONParser jsonParser = new JSONParser();

        try (Stream<String> stream = Files.lines(filePath, StandardCharsets.UTF_8)) {
            stream.forEach(s -> {
                try {
                    contentBuilder.append(jsonParser.parse(s)).append(",");
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (IOException e) {
            System.out.println("Reading the log fails.");
        }

        // Get the log content with a removal of the last comma.
        String fileContent = "[" + contentBuilder.toString().replaceAll(",$", "") + "]";

        return fileContent;

    }



}
