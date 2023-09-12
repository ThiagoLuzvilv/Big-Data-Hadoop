package ex2;

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
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;


// Colocar isso aqui ali nas configurações de RUN: in/transactions_amostra.csv output/ex2.txt


public class Transaction {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "transaction-count");

        // Registro de classes
        j.setJarByClass(Transaction.class); // Classe que tem o main
        j.setMapperClass(MapForTransactionCount.class); // Classe do MAP
        j.setCombinerClass(CombineForTransactionCount.class); // Classe do Combiner
        j.setReducerClass(ReduceForTransactionCount.class); // Classe do Reduce

        // Definir tipos de saída
        // Map
        j.setMapOutputKeyClass(Text.class); // chave
        j.setMapOutputValueClass(IntWritable.class); // valor
        // Reduce
        j.setOutputKeyClass(Text.class); // chave
        j.setOutputValueClass(IntWritable.class); // valor

        // Cadastrar arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // Rodar :)
        j.waitForCompletion(true);
    }

    /**
     * Função de map é chamada POR linha do arquivo de entrada CSV.
     * A chave de saída será uma combinação do tipo de fluxo e do ano.
     */
    public static class MapForTransactionCount extends Mapper<LongWritable, Text,
            Text, IntWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            try {
                // value = linha de entrada CSV
                // obtendo a linha como string
                String linha = value.toString();
                // quebrando a linha em colunas
                String[] colunas = linha.split(";");
                if (colunas.length >= 6) {
                    // obtendo o tipo de fluxo (coluna E) e o ano (coluna B)
                    String tipoFluxo = colunas[5];
                    String ano = colunas[2];
                    // chave de saída será uma combinação do tipo de fluxo e do ano
                    String chaveSaida = tipoFluxo + ";" + ano;
                    con.write(new Text(chaveSaida), new IntWritable(1));
                }
            } catch (Exception e) {

            }
        }
    }

    /**
     * O combiner agrega as contagens locais por tipo de fluxo e ano.
     */
    public static class CombineForTransactionCount extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {
            // loop para somar os valores localmente
            int soma = 0;
            for (IntWritable v : values) {
                soma += v.get();
            }
            // salvando em arquivo
            con.write(key, new IntWritable(soma));
        }
    }
    /**
     * O reducer agrega as contagens por tipo de fluxo e ano e armazena o resultado em um arquivo de texto.
     */
    public static class ReduceForTransactionCount extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {
            // loop para somar os valores da lista
            int soma = 0;
            for (IntWritable v : values) {
                soma += v.get();
            }
            // salvando em arquivo
            con.write(key, new IntWritable(soma));
        }
    }
}
