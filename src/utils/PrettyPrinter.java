package utils;

import java.util.List;

/**
 * Utility class for printing formatted tables.
 */
public class PrettyPrinter {

    /**
     * Prints a formatted table with the specified headers and rows.
     *
     * @param headers The table headers.
     * @param rows    The table rows.
     */
    public static synchronized void printTable(String[] headers, List<String[]> rows) {
        int[] columnWidths = new int[headers.length];
        for (int i = 0; i < headers.length; i++) {
            columnWidths[i] = headers[i].length();
        }

        for (String[] row : rows) {
            for (int i = 0; i < row.length; i++) {
                columnWidths[i] = Math.max(columnWidths[i], row[i].length());
            }
        }

        StringBuilder separator = new StringBuilder("+");
        for (int width : columnWidths) {
            separator.append("-".repeat(width + 2)).append("+");
        }

        System.out.println(separator);
        printRow(headers, columnWidths);
        System.out.println(separator);

        for (String[] row : rows) {
            printRow(row, columnWidths);
        }
        System.out.println(separator);
    }

    /**
     * Prints a single row of the table.
     *
     * @param row         The row data.
     * @param columnWidths The widths of each column.
     */
    private static synchronized void printRow(String[] row, int[] columnWidths) {
        StringBuilder rowBuilder = new StringBuilder("|");
        for (int i = 0; i < row.length; i++) {
            rowBuilder.append(" ")
                      .append(String.format("%-" + columnWidths[i] + "s", row[i]))
                      .append(" |");
        }
        System.out.println(rowBuilder);
    }
}
