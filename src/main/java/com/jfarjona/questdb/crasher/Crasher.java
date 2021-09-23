/*
 * Crasher.java
 * 
 * 
 * Copyright (c) 1991-2021 Juan Felipe Arjona
 *
 * Copyrighted and proprietary code, all rights are reserved.
 * Reproduction, copying, or reuse without written approval is strictly forbidden.
 *
 * @author Juan F. Arjona
 *
 */

package com.jfarjona.questdb.crasher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.tablesaw.api.ColumnType;
import tech.tablesaw.api.Table;
import tech.tablesaw.io.csv.CsvReadOptions;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.format.DateTimeFormatter;

/**
 *
 * @author jfarjona
 */
public class Crasher {
    private static final Logger log    = LoggerFactory.getLogger(Crasher.class);
    DateTimeFormatter           fedfmt = DateTimeFormatter.ofPattern("MM/dd/uuuu");
    DateTimeFormatter           fmt    = DateTimeFormatter.ISO_DATE;
    private TSDatabase          db     = new TSDatabase();

    /**
     * Method description
     *
     *
     * @param args
     */
    public static void main(String[] args) {
        Crasher me = new Crasher();

        me.run();
    }

    /**
     * Method description
     *
     *
     * @param tmpFile
     *
     * @return
     *
     * @throws IOException
     */
    private Table readFile(Path tmpFile) throws IOException {
        Table answ = null;

        if (tmpFile.toFile()
                   .length() > 0) {
            // Now we read the CSV File
            CsvReadOptions opts = CsvReadOptions.builder(tmpFile.toFile())
                                                .dateFormat(fmt)
                                                .columnTypes(
                                                    new ColumnType[] {
                // datetime
                ColumnType.LOCAL_DATE,

                // Close
                ColumnType.DOUBLE,

                // Open
                ColumnType.DOUBLE,

                // High
                ColumnType.DOUBLE,

                // Low
                ColumnType.DOUBLE
            })
                                                .header(true)
                                                .separator(',')
                                                .skipRowsWithInvalidColumnCount(true)
                                                .build();

            answ = Table.read()
                        .csv(opts);
            answ.setName("data");
            answ = answ.sortAscendingOn("datetime");
        }
        return answ;
    }

    /**
     * Main routing
     *
     */
    public void run() {
        try {
            Table data = readFile(Path.of("." + File.separator + "data.csv"));

            if (data != null) {
                if (!db.tableExists("data")) {
                    db.createTable("data", data, "datetime", "MONTH");
                }

                // Store the data
                db.storeTable("data", data, "MONTH", "datetime");

                // Now we make it crash
                db.storeTable("data", data, "MONTH", "datetime");
            }
        } catch (Throwable t) {
            // We never get here
            log.error("Error!", t);
        } finally {
            db.close();
        }
    }
}

/*
 * @(#)Crasher.java   21/09/23
 * 
 * Copyright (c) 1990-2021 Juan F Arjona
 *
 * Reproduction, use, copy or any action done with this code without written authorization from the author is forbidden and will be prosecuted to the maximum extent to the law.
 *
 *
 *
 */
