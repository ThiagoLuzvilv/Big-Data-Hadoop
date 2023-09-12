package ex1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;


// Colocar isso aqui ali nas configurações de RUN: in/transactions_amostra.csv output/contagem.txt


public class WordCount {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "countbrazil");

        // Registro de classes
        j.setJarByClass(WordCount.class); // Classe que tem o main
        j.setMapperClass(MapForCountBrazil.class); // Classe do MAP
        j.setCombinerClass(CombineForCountBrazil.class); // Classe do Combiner
        j.setReducerClass(ReduceForCountBrazil.class); // Classe do Reduce

        // Definir tipos de saída
        // Map
        j.setMapOutputKeyClass(Text.class); // chave
        j.setMapOutputValueClass(IntWritable.class); // valor
        // Reduce
        j.setOutputKeyClass(Text.class); // chave
        j.setOutputValueClass(IntWritable.class); // valor

        // Cadastrar arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // Rodar :)
        j.waitForCompletion(true);
    }

    public static class MapForCountBrazil extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            // value = linha de entrada
            // obtendo a linha como string
            String linha = value.toString();
            // Verificando se a linha contém "Brazil"
            if (linha.contains("Brazil")) {
                con.write(new Text("Brazil"), one);
            }
        }
    }


    // reduz a quantidade de dados transferidos
    public static class CombineForCountBrazil extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class ReduceForCountBrazil extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {
            int count = 0;
            // Iterar sobre as contagens e somá-las
            for (IntWritable value : values) {
                count += value.get();
            }
            con.write(key, new IntWritable(count));
        }
    }
}
