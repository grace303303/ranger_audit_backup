// Copyright (c) 2023 Cloudera, Inc. All rights reserved.
package org.rangeraudit;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

public final class Utilities {
    /**
     * Static methods that will be used for running basic logics.
     */
    private static final Logger LOG = LoggerFactory.getLogger(Utilities.class);

    private Utilities() {
    }

    /**
     * Get the user inputs from CLI.
     *
     * @param args -- A list of the user inputs.
     * @return The user inputs in a Namespace format.
     */
    public static Namespace getUserInputs(String[] args) {
        final Integer threadNumberDefault = 1;
        final Integer documentsPerBatchDefault = 1000;
        final String jaasConfPathDefault = "/run/cloudera-scm-agent/process";
        ArgumentParser parser = ArgumentParsers.newFor("ranger-audits-reindex")
                .build()
                .defaultHelp(true)
                .description("Download ranger audits from Cloud and"
                        + " upload them into Solr.");
        parser.addArgument("--cloud_type")
                .choices("aws", "azure", "AWS", "AZURE", "Aws", "Azure")
                .required(true)
                .help("The cloud type, it should be either AWS or Azure.");
        parser.addArgument("--storage_location")
                .required(true)
                .help("The storage location where the data is stored"
                        + " without the prefix."
                        + " example: my-bucket-name/my-env-name/data");
        parser.addArgument("--solr_path")
                .required(true)
                .help("The Solr path to insert the content."
                        + "example: my-env0.myname.xcu2-8y8x.wl.cloudera"
                        + ".site:8985");
        parser.addArgument("--days_ago")
                .type(Integer.class)
                .required(true)
                .help("How many days ago we want to start downloading the"
                        + " logs.");
        parser.addArgument("--access_key_id")
                .required(true)
                .help("Cloud Access Key ID.");
        parser.addArgument("--secret_access_key")
                .help("AWS Secret Access Key.");
        parser.addArgument("--jaas_conf_path")
                .setDefault(jaasConfPathDefault)
                .help("The path of the jaas_conf_path.");
        parser.addArgument("--region")
                .help("The region of the cloud, for example us-west-2.");
        parser.addArgument("--threads")
                .setDefault(threadNumberDefault)
                .type(Integer.class)
                .help("Number of threads to process Solr insertion.");
        parser.addArgument("--documents_per_batch")
                .setDefault(documentsPerBatchDefault)
                .type(Integer.class)
                .help("Number of documents per batch when inserting into "
                        + "Solr.");
        Namespace res = null;
        try {
            res = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }
        return res;
    }

    /**
     * Get the date to start downloading files based on how many days ago.
     *
     * @param daysAgo -- How many days ago we want to start downloading the
     *                logs, for example, put "0" will return today's date,
     *                and put "2" will return the date before yesterday.
     * @return the date based on the number of days ago.
     */
    public static LocalDate getDaysAgoDate(int daysAgo) {
        LocalDate todayDate = LocalDate.now();
        return todayDate.minusDays(daysAgo);

    }

    /**
     * Determine if a string is in a date format or not.
     *
     * @param potentialDate -- A String converted from a directory name.
     * @return true or false, for example, If the passed in String is
     * "20230101", it returns true, and if it is "test", it returns false.
     */
    public static boolean isDateStr(String potentialDate) {
        DateTimeFormatter dateFormatter = DateTimeFormatter.BASIC_ISO_DATE;

        try {
            LocalDate.parse(potentialDate, dateFormatter);
        } catch (DateTimeParseException e) {
            return false;
        }

        return true;
    }

    /**
     * Determine if `potentialDate` is a date on or after the date days ago,
     * for example, if today is 20230101, and daysAgo is 2.
     * This will return true if potentialDate is 20221230 but false if
     * potentialDate is 20221229.
     * @param potentialDate -- The directory that is a date.
     * @param daysAgo -- How many days ago we want to start getting the data.
     * @return true or false.
     */
    public static boolean isLaterDate(String potentialDate,
            final int daysAgo) {
        if (!isDateStr(potentialDate)) {
            return false;
        }

        //convert String, for example "20230112", to LocalDate.
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        LocalDate newPotentialDate = LocalDate.parse(potentialDate, formatter);

        LocalDate dateDaysAgo = getDaysAgoDate(daysAgo);

        return newPotentialDate.equals(dateDaysAgo)
                || newPotentialDate.isAfter(dateDaysAgo);

    }

    /**
     * Delete the tmp_logs directory.
     * @param directory -- The tmp_logs directory that is used to store
     *                  the downloaded files temporarily.
     */
    public static void deleteDirectory(File directory)
            throws IOException {
        if (!directory.isDirectory()) {
            return;
        }
        FileUtils.deleteDirectory(directory);
        LOG.info("Deleted directory " + directory + ".");
    }

    /**
     * Delete the downloaded file.
     * @param file -- The path of the file, for example
     *             tmp_logs/20230228/XYZ.log
     */
    public static void deleteLogFile(File file) {
        if (!file.isFile()) {
            return;
        }
        if (file.delete()) {
            LOG.info("Deleted log file " + file + ".");
        } else {
            LOG.info("Failed to delete log file " + file + ".");
        }
    }

    /**
     * Get the path of jaas config file.
     * @param jaasConfPath -- The path where the file is, this is defaulted
     *                        to be "/run/cloudera-scm-agent/process".
     * @return The full path of the jaas config file.
     */
    public static String getJaasConf(String jaasConfPath) {
        String findCommand = String.format("find %s -name solr.keytab "
                + "| tail -n 1", jaasConfPath);
        try {
            ProcessBuilder processBuilder = new ProcessBuilder(
                    "/bin/sh", "-c", findCommand);
            Process process = processBuilder.start();
            BufferedReader bufferedReader = new BufferedReader(
                    new InputStreamReader(process.getInputStream()));
            String strCurrentLine;
            while ((strCurrentLine = bufferedReader.readLine()) != null) {
                return strCurrentLine.replace(
                        "solr.keytab", "jaas.conf");
            }
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
        return "";
    }
}
