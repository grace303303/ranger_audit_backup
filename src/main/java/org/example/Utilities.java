package org.example;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.security.PrivateKey;
import java.util.ArrayList;
import java.util.Date;
import java.time.LocalDate;

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

}
