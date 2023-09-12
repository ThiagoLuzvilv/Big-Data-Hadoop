package ex4;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

public class VendaBrasil {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();


        Configuration conf = new Configuration();
        String[] files = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Arquivo de entrada
        Path input = new Path("in/transactions_amostra.csv");

        // Arquivo de saída
        Path output = new Path("output/ex4");

        Job j = new Job(conf, "VendaBrasil");

        j.setJarByClass(VendaBrasil.class);
        j.setMapperClass(MapForTotalPrice.class);
        j.setReducerClass(ReduceForAveragePrice.class);

        j.setCombinerClass(CombinerForAveragePrice.class); // Adicionando o Combiner

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(DoubleWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        j.waitForCompletion(true);
    }

    public static class MapForTotalPrice extends Mapper<Object, Text, Text, DoubleWritable> {
        private Text outputKey = new Text();
        private DoubleWritable outputValue = new DoubleWritable();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] columns = value.toString().split(";");
            String unit = columns[7];
            String year = columns[1];
            String category = columns[9];
            String country = columns[0];
            String flow = columns[4];
            String price = columns[5];
                if (country.equals("Brazil") && flow.equals("Export")) {
                    try {
                        double preco = Double.parseDouble(price);
                        String groupKey = unit + ";" + year + ";" + category;
                        outputKey.set(groupKey);
                        outputValue.set(preco);
                        context.write(outputKey, outputValue);
                    } catch (Exception e) {
                        // Lidar com erros de análise ou outras exceções
                }
            }
        }
    }

    public static class CombinerForAveragePrice extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            double count = 0;
            for (DoubleWritable value : values) {
                sum += value.get();
                count++;
            }
            if (count > 0) {
                double average = sum / count;
                result.set(average);
                context.write(key, result);
            }
        }
    }

    public static class ReduceForAveragePrice extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            double count = 0;
            for (DoubleWritable value : values) {
                sum += value.get();
                count++;
            }
            if (count > 0) {
                double average = sum / count;
                result.set(average);
                context.write(key, result);
            }
        }
    }
}