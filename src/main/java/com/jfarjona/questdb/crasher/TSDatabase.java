/*
 * TSDatabase.java
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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.TableWriter;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.NumericException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;
import tech.tablesaw.api.BooleanColumn;
import tech.tablesaw.api.DateColumn;
import tech.tablesaw.api.DateTimeColumn;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.FloatColumn;
import tech.tablesaw.api.InstantColumn;
import tech.tablesaw.api.IntColumn;
import tech.tablesaw.api.LongColumn;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.ShortColumn;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;
import java.io.File;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/** @author jfarjona */
public class TSDatabase {
    private static final Logger                                              log          = LoggerFactory.getLogger(
                                                                                                TSDatabase.class);
    private static final ZoneId                                              EST          = TimeZone.getTimeZone(
                                                                                                "America/New York")
                                                                                                    .toZoneId();
    private static final long                                                START_NANOS  = Instant.parse(
                                                                                                "1970-01-01T00:00:00Z")
                                                                                                   .toEpochMilli() * 1000L
                                                                                                                   * 1000L;
    private static final long                                                START_MICROS = Instant.parse(
                                                                                                "1970-01-01T00:00:00Z")
                                                                                                   .toEpochMilli() * 1000L;
    private static final long                                                START_MILLIS = Instant.parse(
                                                                                                "1970-01-01T00:00:00Z")
                                                                                                   .toEpochMilli();
    private static final long                                                MAX_MILLIS   = Instant.parse(
                                                                                                "2100-01-01T00:00:00Z")
                                                                                                   .toEpochMilli();
    private CairoEngine                                                      engine;
    private Map<String, FourConsumer<Row, String, TableWriter.Row, Integer>> STORE_CONVERTERS;
    private Map<String, FourConsumer<Record, Integer, Row, String>>          READER_CONVERTERS;

    /**
     * Constructs ...
     *
     */
    public TSDatabase() {
        init();
    }

    /**
     * Method description
     *
     */
    private void buildLists() {
        STORE_CONVERTERS = new HashMap<>();
        STORE_CONVERTERS.put(
            "LOCAL_DATE_TIME",
                (Row rt, String cname, TableWriter.Row rw, Integer idx) -> {
                    rw.putTimestamp(idx, rt.getDateTime(cname)
                                           .atZone(EST)
                                           .toInstant()
                                           .toEpochMilli());
                });
        STORE_CONVERTERS.put(
            "INSTANT",
                (Row rt, String cname, TableWriter.Row rw, Integer idx) -> {
                    rw.putTimestamp(idx, rt.getInstant(cname)
                                           .toEpochMilli());
                });
        STORE_CONVERTERS.put(
            "LOCAL_DATE",
                (Row rt, String cname, TableWriter.Row rw, Integer idx) -> {
                    rw.putDate(idx, rt.getDate(cname)
                                      .atStartOfDay()
                                      .atZone(EST)
                                      .toInstant()
                                      .toEpochMilli());
                });
        STORE_CONVERTERS.put(
            "LOCAL_TIME",
                (Row rt, String cname, TableWriter.Row rw, Integer idx) -> {
                    rw.putLong(idx, rt.getTime(cname)
                                      .toNanoOfDay());
                });
        STORE_CONVERTERS.put(
            "LONG",
                (Row rt, String cname, TableWriter.Row rw, Integer idx) -> {
                    rw.putLong(idx, rt.getLong(cname));
                });
        STORE_CONVERTERS.put(
            "INTEGER",
                (Row rt, String cname, TableWriter.Row rw, Integer idx) -> {
                    rw.putInt(idx, rt.getInt(cname));
                });
        STORE_CONVERTERS.put(
            "SHORT",
                (Row rt, String cname, TableWriter.Row rw, Integer idx) -> {
                    rw.putShort(idx, rt.getShort(cname));
                });
        STORE_CONVERTERS.put(
            "FLOAT",
                (Row rt, String cname, TableWriter.Row rw, Integer idx) -> {
                    rw.putFloat(idx, rt.getFloat(cname));
                });
        STORE_CONVERTERS.put(
            "DOUBLE",
                (Row rt, String cname, TableWriter.Row rw, Integer idx) -> {
                    rw.putDouble(idx, rt.getDouble(cname));
                });
        STORE_CONVERTERS.put(
            "BOOLEAN",
                (Row rt, String cname, TableWriter.Row rw, Integer idx) -> {
                    rw.putBool(idx, rt.getBoolean(cname));
                });
        STORE_CONVERTERS.put(
            "STRING",
                (Row rt, String cname, TableWriter.Row rw, Integer idx) -> {
                    rw.putStr(idx, rt.getString(cname));
                });
        STORE_CONVERTERS.put(
            "TEXT",
                (Row rt, String cname, TableWriter.Row rw, Integer idx) -> {
                    rw.putStr(idx, rt.getString(cname));
                });
        READER_CONVERTERS = new HashMap<>();
        READER_CONVERTERS.put(
            "LOCAL_DATE_TIME",
                (Record r, Integer idx, Row rt, String cname) -> {
                    rt.setDateTime(cname, Instant.ofEpochMilli(r.getTimestamp(idx))
                                                 .atZone(EST)
                                                 .toLocalDateTime());
                });
        READER_CONVERTERS.put(
            "INSTANT",
                (Record r, Integer idx, Row rt, String cname) -> {
                    rt.setInstant(cname, Instant.ofEpochMilli(r.getTimestamp(idx)));
                });
        READER_CONVERTERS.put(
            "LOCAL_DATE",
                (Record r, Integer idx, Row rt, String cname) -> {
                    rt.setDate(cname, Instant.ofEpochMilli(r.getTimestamp(idx))
                                             .atZone(EST)
                                             .toLocalDate());
                });
        READER_CONVERTERS.put(
            "LOCAL_TIME",
                (Record r, Integer idx, Row rt, String cname) -> {
                    rt.setTime(cname, LocalTime.ofNanoOfDay(r.getLong(idx)));
                });
        READER_CONVERTERS.put(
            "LONG",
                (Record r, Integer idx, Row rt, String cname) -> {
                    rt.setLong(cname, r.getLong(idx));
                });
        READER_CONVERTERS.put(
            "INTEGER",
                (Record r, Integer idx, Row rt, String cname) -> {
                    rt.setInt(cname, r.getInt(idx));
                });
        READER_CONVERTERS.put(
            "SHORT",
                (Record r, Integer idx, Row rt, String cname) -> {
                    rt.setShort(cname, r.getShort(idx));
                });
        READER_CONVERTERS.put(
            "FLOAT",
                (Record r, Integer idx, Row rt, String cname) -> {
                    rt.setFloat(cname, r.getFloat(idx));
                });
        READER_CONVERTERS.put(
            "DOUBLE",
                (Record r, Integer idx, Row rt, String cname) -> {
                    rt.setDouble(cname, r.getDouble(idx));
                });
        READER_CONVERTERS.put(
            "BOOLEAN",
                (Record r, Integer idx, Row rt, String cname) -> {
                    rt.setBoolean(cname, r.getBool(idx));
                });
        READER_CONVERTERS.put(
            "STRING",
                (Record r, Integer idx, Row rt, String cname) -> {
                    rt.setString(cname, r.getStr(idx)
                                         .toString());
                });
        READER_CONVERTERS.put(
            "TEXT",
                (Record r, Integer idx, Row rt, String cname) -> {
                    rt.setText(cname, r.getStr(idx)
                                       .toString());
                });
    }

    /** Method description */
    public void close() {
        if (engine != null) {
            engine.close();
        }
    }

    /**
     * Creates a table with the column names and types as the given jsaw Table
     * @param name the name for the new table
     * @param tbl the jsaw table
     * @param timeStampColumn the name of the column with the timestamp to use
     * @param partition the way to partition the new table (SECOND, MINUTE, HOUR, DAY, MONTH, DAY, YEAR) -- see questdb
     */
    public void createTable(String name, Table tbl, String timeStampColumn, String partition) {
        final String[] JSAW_TYPES = {
            "LOCAL_DATE_TIME", "INSTANT", "LOCAL_DATE", "LOCAL_TIME", "LONG", "INTEGER", "SHORT", "FLOAT", "DOUBLE",
            "BOOLEAN", "STRING", "TEXT"
        };
        final String[] SQL_TYPES = {
            "timestamp", "timestamp", "timestamp", "timestamp", "long", "int", "short", "float", "double", "boolean",
            "string", "string"
        };
        SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1);

        // Holds the name and the sql type for each column
        List<Tuple2<String, String>> columns = new ArrayList<>();
        List<Column>                 tblCols = new ArrayList<>();

        tbl.columns()
           .forEach(
               c -> {
                   String cName   = c.name();
                   String sqlType = null;

                   for (int i = 0; i < JSAW_TYPES.length; i++) {
                       if (JSAW_TYPES[i].equalsIgnoreCase(c.type()
                                                           .name())) {
                           sqlType = SQL_TYPES[i];
                           break;
                       }
                   }
                   if (sqlType != null) {
                       Tuple2<String, String> cAdd = Tuples.of(fixColumnName(cName), sqlType);

                       columns.add(cAdd);
                       tblCols.add(c);
                   }
               });

        // Pick the timestamp
        Comparator<Column> comp =
            (c1, c2) -> {
                int p1 = findInArray(JSAW_TYPES, c1.type()
                                                   .name());
                int p2 = findInArray(JSAW_TYPES, c2.type()
                                                   .name());

                return Integer.compare(p1, p2);
            };

        Collections.sort(tblCols, comp);

        String tstamp = timeStampColumn;

        if (tstamp == null) {
            tstamp = (tblCols.size() > 0)
                     ? tblCols.get(0)
                              .name()
                     : null;
        }
        if (tstamp != null) {
            StringBuilder sql = new StringBuilder();

            sql.append("create table '")
               .append(fixColumnName(name))
               .append("' (");
            columns.forEach(
                ss -> {
                    sql.append("'")
                       .append(ss.getT1())
                       .append("' ")
                       .append(ss.getT2())
                       .append(", ");
                });
            if (sql.length() > 1) {
                sql.delete(sql.length() - 2, sql.length());
            }
            sql.append(") timestamp (")
               .append(tstamp)
               .append(") ");
            if (partition != null) {
                sql.append("PARTITION BY ")
                   .append(partition);
            }
            try (SqlCompiler compiler = new SqlCompiler(engine)) {
                compiler.compile(sql.toString(), ctx);
            } catch (SqlException ex) {
                log.error("Cannot execute the SQL statement: {}", sql, ex);
            }
        } else {
            log.error("Cannot find a timestamp column!");
            throw new IllegalArgumentException("Invalid timestamp column for table named " + tbl.name());
        }
    }

    /**
     * Issues a DROP TABLE name statement if the table exists.  Otherwise it does nothing.
     *
     *
     * @param name the table to drop
     */
    public void dropTable(String name) {
        name = fixColumnName(name);
        if (tableExists(name)) {
            SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1);
            String                  sql = "drop table '" + name + "';";

            try (SqlCompiler compiler = new SqlCompiler(engine)) {
                compiler.compile(sql, ctx);
            } catch (SqlException ex) {
                log.error("Cannot execute the SQL statement: {}", sql, ex);
            }
        }
    }

    /**
     * Returns the first position of s in the array.
     * @param array the array to search within (using equalsIgnoreCase)
     * @param s the value sought
     * @return the position, -1 if not found
     */
    private int findInArray(String[] array, String s) {
        int answ = -1;

        for (int i = 0; i < array.length; i++) {
            if (s.equalsIgnoreCase(array[i])) {
                answ = i;
                break;
            }
        }
        return answ;
    }

    /**
     * Replaces invalid characters in the table name
     * @param tColname has the column name in the table
     * @return the same name, but with the invalid characters replaced by underscores
     */
    private String fixColumnName(String tColname) {
        String answ = tColname;

        answ = answ.replace("-", "_");
        answ = answ.replace(".", "_");
        answ = answ.replace("~", "_");
        answ = answ.replace("/", "_");
        answ = answ.replace("\\", "_");
        answ = answ.replace("!", "_");
        answ = answ.replace("$", "_");
        answ = answ.replace("`", "_");
        answ = answ.replace("'", "_");
        answ = answ.replace("\"", "_");
        answ = answ.replace("|", "_");
        answ = answ.replace("?", "_");
        answ = answ.replace("*", "_");
        answ = answ.replace("{", "_");
        answ = answ.replace("}", "_");
        return answ;
    }

    /** Method description */
    protected void init() {
        buildLists();

        File folder = new File("./questdb");

        if (!folder.exists()) {
            folder.mkdirs();
        }

        CairoConfiguration configuration = new DefaultCairoConfiguration(folder.getAbsolutePath());

        engine = new CairoEngine(configuration);
    }

    ;

    /**
     * Stores the given table under the given name.If the table doesn't exist is created, otherwise data is appended.  Be careful: no checks for column names are done, unless the table is created
     * @param name the table name to use
     * @param table the table with the data to store
     * @param partition has the way in which the table should be partitioned (see questdb). If null, no partition is used.
     * @param timeStampColumn the column name that has the timestamp information
     */
    public void storeTable(String name, Table table, String partition, String timeStampColumn) {
        name = fixColumnName(name);
        if (!tableExists(name)) {
            createTable(name, table, timeStampColumn, partition);
        }
        if (table != null) {
            SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1);

            try (TableWriter writer = engine.getWriter(ctx.getCairoSecurityContext(), name, "writing")) {
                final String tstamp    = writer.getDesignatedTimestampColumnName();
                final Column tstC      = table.column(tstamp);
                final String tsColName = (timeStampColumn == null)
                                         ? tstamp
                                         : timeStampColumn;
                final String nname     = name;

                table.forEach(
                    r -> {
                        try {
                            Instant i = null;

                            switch (tstC.type()
                                        .name()) {
                             case "LOCAL_DATE" : {
                                  i = r.getDate(tsColName)
                                       .atStartOfDay()
                                       .atZone(EST)
                                       .toInstant();
                                  break;
                              }

                             case "LOCAL_DATE_TIME" : {
                                  i = r.getDateTime(tsColName)
                                       .atZone(EST)
                                       .toInstant();
                                  break;
                              }

                             case "INSTANT" : {
                                  i = r.getInstant(tsColName);
                                  break;
                              }

                             case "LONG" :
                             case "SHORT" :
                             case "INTEGER" : {
                                  i = Instant.ofEpochMilli(r.getLong(tsColName));
                                  break;
                              }
                            }
                            if (i != null) {
                                long            ts  = TimestampFormatUtils.parseTimestamp(i.toString());
                                TableWriter.Row row = writer.newRow(ts);

                                for (String cName : r.columnNames()) {
                                    int                                                 dbIdx = writer.getColumnIndex(
                                                                                                    fixColumnName(
                                                                                                        cName));
                                    FourConsumer<Row, String, TableWriter.Row, Integer> str   = STORE_CONVERTERS.get(
                                                                                                    r.getColumnType(
                                                                                                        cName)
                                                                                                     .name());

                                    if (str != null) {
                                        str.accept(r, cName, row, dbIdx);
                                    }
                                }
                                row.append();
                                writer.commit();
                            } else {
                                log.error("No timestamp found for column {}", tstamp);
                            }
                        } catch (NumericException ex) {
                            log.error("Cannot parse the date {}", r.getDateTime("datetime"));
                        } catch (Exception ex) {
                            log.error("Cannot write to table {}!", nname, ex);
                        }
                    });
            }
        }
    }

    /**
     * Checks if the given table name exists.
     *
     * @param name the table to check
     * @return true if the table exists, false otherwise
     */
    public boolean tableExists(String name) {
        name = fixColumnName(name);

        SqlExecutionContextImpl ctx  = new SqlExecutionContextImpl(engine, 1);
        String                  sql  = "SHOW TABLES;";
        boolean                 answ = false;

        try (SqlCompiler compiler = new SqlCompiler(engine)) {
            try (RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor(ctx)) {
                    final Record record = cursor.getRecord();

                    while (cursor.hasNext()) {
                        String tName = record.getStr(0)
                                             .toString();

                        if (tName.equalsIgnoreCase(name)) {
                            answ = true;
                            break;
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("Cannot query the timeseries database!", e);
        }
        return answ;
    }

    /**
     * Method description
     *
     *
     * @param nanoMilliMicros
     *
     * @return
     */
    public static Instant toInstantAuto(long nanoMilliMicros) {
        // are we in millis?
        long MAX_DELTA_MILLIS = MAX_MILLIS - START_MILLIS;
        long MAX_DELTA_MICROS = MAX_MILLIS * 1000L - START_MICROS;

        // is it millis?
        if ((nanoMilliMicros - START_MILLIS) > MAX_DELTA_MILLIS) {
            // is it micros?
            if ((nanoMilliMicros - START_MICROS) > MAX_DELTA_MICROS) {
                // it is nanos.
                return toInstantMicro(nanoMilliMicros);
            } else {
                // it is micros
                return toInstantMicro(nanoMilliMicros);
            }
        } else {
            // it is millis
            return toInstantMilli(nanoMilliMicros);
        }
    }

    /**
     * Method description
     *
     *
     * @param micros
     *
     * @return
     */
    public static Instant toInstantMicro(long micros) {
        Instant answ = toInstantMilli(micros / 1000L);

        return answ;
    }

    /**
     * Method description
     *
     *
     * @param millis
     *
     * @return
     */
    public static Instant toInstantMilli(long millis) {
        Instant answ = Instant.ofEpochMilli(millis);

        return answ;
    }

    /**
     * Converts a nanos timestamp into an instant
     *
     * @param nanos the nanos from the DB
     * @return the equivalent instant
     */
    public static Instant toInstantNanos(long nanos) {
        Instant answ = toInstantMicro(nanos / 1000L);

        return answ;
    }

    /**
     * Converts the nanos to a Local Date time using TZ as EST
     *
     * @param nanosMicroMillis the timestamp in nanos, millis or micro seconds
     * @return the LocalDateTime equivalent, null if the nanos is < 0
     */
    public static LocalDateTime toLocalDateTime(long nanosMicroMillis) {
        if (nanosMicroMillis >= 0) {
            return toInstantAuto(nanosMicroMillis).atZone(EST)
                                                  .toLocalDateTime();
        } else {
            return null;
        }
    }

    /**
     * Converts the instant into nanoseconds time
     *
     * @param ins the instant to convert
     * @return the nanos timestamp
     */
    public static long toNanos(Instant ins) {
        return (ins.toEpochMilli() * 1000000L);
    }
}

/*
 * @(#)TSDatabase.java   21/09/23
 * 
 * Copyright (c) 1990-2021 Juan F Arjona
 *
 * Reproduction, use, copy or any action done with this code without written authorization from the author is forbidden and will be prosecuted to the maximum extent to the law.
 *
 *
 *
 */
