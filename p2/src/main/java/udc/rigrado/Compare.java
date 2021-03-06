package udc.rigrado;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import org.apache.commons.math3.stat.inference.TTest;

import java.io.FileReader;
import java.io.IOException;
import org.apache.commons.math3.stat.inference.WilcoxonSignedRankTest;
import org.apache.commons.math3.stat.ranking.NaNStrategy;
import org.apache.commons.math3.stat.ranking.TiesStrategy;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Compare {
    private enum Metrica {P, R, MAP}
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
        if(results1 == null || results2 == null){
            System.err.println("Usage:" + usage);
            System.exit(1);
        }

        if(alph <= 0f){
            System.err.println("alfa must be a non negative number");
            System.exit(1);
        }

        if (validateCsv(results1, results2)){
            System.out.println("Valid CSVs");
        }

        CSVReader reader1 = null;
        CSVReader reader2 = null;
        try {
             reader1 = new CSVReader( new FileReader("./"+results1));
             reader2 = new CSVReader( new FileReader("./"+results2));
        } catch (IOException e) {
            e.printStackTrace();
        }

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
        } catch (IOException | CsvException e) {
            throw new RuntimeException(e);
        }
        if(sizeTable1 != sizeTable2){
            System.err.println("Csv file size does not match");
            System.exit(1);
        }

        double[] table1 = new double[sizeTable1];
        double[] table2 = new double[sizeTable2];
        double pValue = 0;
        Boolean testResult = false;

        Metrica metric = Metrica.valueOf(new CsvProperties(results1).metric.toUpperCase());
        int metricCol = 0;
        switch (metric.toString()){
            case "MAP":
            case "AP":
                metricCol = 3;
                break;
            case "P":
                metricCol = 1;
                break;
            case "R":
                metricCol = 2;
                break;

            default:
                break;
        }

        for(int i = 1; i < content1.size()-1; i++) {
            table1[i-1] = Double.parseDouble(content1.get(i)[metricCol]);
        }
        for(int i = 1; i < content2.size()-1; i++) {
            table2[i-1] = Double.parseDouble(content2.get(i)[metricCol]);
        }
        if (metric == Metrica.MAP){

            double[] double1 = new double[table1.length];
            double[] double2 = new double[table2.length];

            for (int i = 0; i < table1.length; i++) {
                double sum1 = 0;
                double sum2 = 0;
                sum1 += table1[i];
                double1[i] = sum1/(i+1);
                sum2 += table2[i];
                double2[i] = sum2/(i+1);

            }

            if (testType == TestType.T) {
                TTest tTest = new TTest();
                pValue = tTest.tTest(double1, double2);
                testResult = tTest.pairedTTest(double1, double2, alph);

            } else {
                WilcoxonSignedRankTest wilcoxon = new WilcoxonSignedRankTest(NaNStrategy.FIXED, TiesStrategy.AVERAGE);
                pValue = wilcoxon.wilcoxonSignedRankTest(double1, double2, false);
                testResult = pValue < alph;

            }

        }else{
            if (testType == TestType.T) {
                TTest tTest = new TTest();
                pValue = tTest.pairedTTest(table1, table2);
                testResult = tTest.pairedTTest(table1, table2, alph);

            } else {
                WilcoxonSignedRankTest wilcoxon = new WilcoxonSignedRankTest(NaNStrategy.FIXED,TiesStrategy.AVERAGE);
                pValue = wilcoxon.wilcoxonSignedRankTest(table1, table2, false);
                testResult = pValue < alph;

            }
        }

        System.out.println("Result from " + testType.toString().toLowerCase() + " test with alpha = " + alph  + ":");
        String sout = testResult ? "Null hypothesis rejected" : "Null hypothesis accepted";

        sout +=  "\t| pvalue = " + pValue;
        System.out.println(sout);

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
