package ex3;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

public class PriceValue {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration conf = new Configuration();
        String[] files = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Arquivo de entrada
        Path input = new Path(files[0]);

        // Arquivo de saída
        Path output = new Path(files[1]);

        // Criação do job e configurações
        Job job = new Job(conf, "price-value");

        // Registro de classes
        job.setJarByClass(PriceValue.class);
        job.setMapperClass(MapForTotalPrice.class);
        job.setReducerClass(ReduceForAveragePrice.class);

        // Registrar o Combiner
        job.setCombinerClass(CombinerForAveragePrice.class);

        // Definir tipos de saída
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Registrar arquivos de entrada e saída
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        // Executar o job
        job.waitForCompletion(true);
    }

    public static class MapForTotalPrice extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            try {
                String linha = value.toString();
                String[] colunas = linha.split(";");
                if (colunas.length >= 3) {
                    String pais = colunas[0].trim();
                    String ano = colunas[1].trim();
                    String valorTotalStr = colunas[2].replace(",", ".").trim();

                    // Verificar se o valor é um número válido e positivo
                    if (isPositiveNumeric(valorTotalStr)) {
                        double valorTotal = Double.parseDouble(valorTotalStr);
                        String chave = ano + "," + pais;
                        context.write(new Text(chave), new DoubleWritable(valorTotal));
                    }
                }
            } catch (Exception e) {
                // Lidar com erros
            }
        }

        private boolean isPositiveNumeric(String str) {
            try {
                double value = Double.parseDouble(str);
                return value >= 0.0;
            } catch (NumberFormatException e) {
                return false;
            }
        }
    }

    public static class ReduceForAveragePrice extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double soma = 0.0;
            int count = 0;
            for (DoubleWritable value : values) {
                soma += value.get();
                count++;
            }
            // Calcular a média apenas se houver valores válidos
            if (count > 0) {
                double media = soma / count;
                context.write(key, new DoubleWritable(media));
            }
        }
    }

    public static class CombinerForAveragePrice extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double soma = 0.0;
            int count = 0;
            for (DoubleWritable value : values) {
                soma += value.get();
                count++;
            }
            // Calcular a média apenas se houver valores válidos
            if (count > 0) {
                double media = soma / count;
                context.write(key, new DoubleWritable(media));
            }
        }
    }
}
