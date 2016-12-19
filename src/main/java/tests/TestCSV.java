package tests;

import com.google.common.collect.Iterables;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.StringEscapeUtils;

import java.io.IOException;
import java.io.StringReader;

/**
 * Created by ssoldatov on 11/16/16.
 */
public class TestCSV {

    public static void main(String[] args) throws IOException {
        Character quoteChar = '"';
            String quoteString = "";
        String xxx = StringEscapeUtils.unescapeJava("\\u0000");
        char xxxxx = xxx.charAt(0);
        quoteChar = new Character((char) 0xFFFF);
            if(quoteString.length() == 0) {
                quoteChar = null;
            } else if (quoteString.length() != 1) {
                throw new IllegalArgumentException("Illegal quote character: " + quoteString);
            }
            quoteChar = quoteString.charAt(0);
        CSVFormat format = CSVFormat.DEFAULT.withIgnoreEmptyLines(true).withDelimiter(',')
                .withEscape('\\').withQuote(null);
        CSVParser csvParser = new CSVParser(new StringReader("test,\"test\""), format);
        CSVRecord x = Iterables.getFirst(csvParser, null);
        System.out.println(x);
    }
}
