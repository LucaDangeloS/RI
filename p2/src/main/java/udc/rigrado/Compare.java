package udc.rigrado;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import com.opencsv.exceptions.CsvValidationException;
import org.apache.commons.math3.stat.inference.TTest;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import com.opencsv.CSVReader;
import org.apache.commons.math3.stat.inference.WilcoxonSignedRankTest;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Compare {

    private enum TestType{T, WILCOXON}

    // Csv properties class data type
    private static class CsvProperties {
        public final boolean isValid;
        String model;
        String trainingRange;
        String testRange;
        String metric;
        String cut;
        String fase;

        public CsvProperties(String model, String trainingRange, String testRange, String metric, String cut, String fase) {
            this.model = model;
            this.trainingRange = trainingRange;
            this.testRange = testRange;
            this.metric = metric;
            this.cut = cut;
            this.fase = fase;
            this.isValid = true;
        }

        public CsvProperties(String name) {
            String pattern = "\t*medline\\.(jm|tfidf)\\.training\\.(\\d+-\\d+|null)\\.test\\.(\\d+-\\d+)\\.(map|p|r)(\\d+)\\.(training|test)\\.csv";
            Pattern r = Pattern.compile(pattern);
            Matcher m = r.matcher(name);
            if (m.find()) {
                this.model = m.group(1);
                this.trainingRange = m.group(2);
                this.testRange = m.group(3);
                this.metric = m.group(4);
                this.cut = m.group(5);
                this.fase = m.group(6);
                this.isValid = true;
            } else {
                this.isValid = false;
            }
        }
    }

    public static void main(String[] args) {
        String usage = "java Compare"
                + " [-results results1.csv results2.csv] [-test t|wilcoxon alpha] ";
        String results1 = null;
        String results2 = null;

        String tesTypeStr;
        TestType testType = null;
        Float alph = 0f;
        try {
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "-results":
                        results1 = args[++i];
                        results2 = args[++i];
                        break;
                    case "-test":
                        try {
                            testType = TestType.valueOf(args[++i].toUpperCase());
                        } catch (IllegalArgumentException e) {
                            throw new IllegalArgumentException(e);
                        }
                        alph = Float.valueOf(args[++i]);
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown parameter " + args[i]);
                }
            }
        } catch (IllegalArgumentException e) {
            System.err.println("Usage: " + usage);
            System.exit(1);
        }
        if(alph <= 0f){
            System.err.println("alfa must be a non negative number");
            System.exit(1);
        }
        assert results1 != null;
        assert results2 != null;

        if (validateCsv(results1, results2)){
            System.out.println("CSV correctos");

        }

        CSVReader reader1 = null;
        CSVReader reader2 = null;
        try {
             reader1 = new CSVReader( new FileReader("./"+results1));
             reader2 = new CSVReader( new FileReader("./"+results2));
        } catch (IOException e) {
            e.printStackTrace();
        }
        String[] lineInArray;
        if(reader1 == null || reader2 == null ){
            System.err.println("There is a csv file missing");
            System.exit(1);
        }
        int sizeTable1 = 0;
        int sizeTable2 = 0;
        List<String[]> content1 = null;
        List<String[]> content2 = null;
        try {
            content1 = reader1.readAll();
            content2 = reader2.readAll();
            sizeTable1 = content1.size() - 2;
            sizeTable2 = content2.size() - 2;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (CsvException e) {
            throw new RuntimeException(e);
        }


        double[] table1 = new double[sizeTable1];
        double[] table2 = new double[sizeTable2];
        double pValue = 0;

        for(int i = 1; i < content1.size()-1; i++) {
            table1[i-1] = Double.parseDouble(content1.get(i)[3]);
        }
        for(int i = 1; i < content2.size()-1; i++) {
            table2[i-1] = Double.parseDouble(content2.get(i)[3]);
        }
        if(testType == TestType.T){
            TTest tTest = new TTest();
            pValue = tTest.pairedTTest(table1, table2);

        } else {
            WilcoxonSignedRankTest wilcoxon = new WilcoxonSignedRankTest();
            pValue = wilcoxon.wilcoxonSignedRank(table1, table2);
        }

        System.out.println("pvalue = " + pValue);


    }


    public static boolean validateCsv(String result1, String result2 ) {
        CsvProperties csv1 = new CsvProperties(result1);
        CsvProperties csv2 = new CsvProperties(result2);

        if (!csv1.isValid || !csv2.isValid) {
            return false;
        }

        return csv1.metric.equals(csv2.metric) && csv1.testRange.equals(csv2.testRange);
    }

}
