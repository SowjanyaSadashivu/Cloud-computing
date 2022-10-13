import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Triple implements Writable {
    public int i;
    public int j;
    public double value;
    
    Triple () {}
    
    Triple ( int i, int j, double v ) { this.i = i; this.j = j; value = v; }
    
    public void write ( DataOutput out ) throws IOException {
        out.writeInt(i);
        out.writeInt(j);
        out.writeDouble(value);
    }

    public void readFields ( DataInput in ) throws IOException {
        i = in.readInt();
        j = in.readInt();
        value = in.readDouble();
    }

    @Override
    public String toString () {
        return ""+i+"\t"+j+"\t"+value;
    }
}

class Block implements Writable {
    final static int rows = 100;
    final static int columns = 100;
    public double[][] data;

    Block () {
        data = new double[rows][columns];
    }

    public void write ( DataOutput out ) throws IOException {
        for(int i = 0; i < rows; i++){
            for (int j = 0; j<columns; j++){
                out.writeDouble(data[i][j]);
            }
        }
    }

    public void readFields ( DataInput in ) throws IOException {
         for(int i = 0; i < rows; i++){
            for (int j = 0; j<columns; j++){
                data[i][j] = in.readDouble();
            }
        }
    }

    @Override
    public String toString () {
        String s = "\n";
        for ( int i = 0; i < rows; i++ ) {
            for ( int j = 0; j < columns; j++ )
                s += String.format("\t%.3f",data[i][j]);
            s += "\n";
        }
        return s;
    }
}

class Pair implements WritableComparable<Pair> {
    public int i;
    public int j;
    
    Pair () {}
    
    Pair ( int i, int j ) { this.i = i; this.j = j; }
    
    public void write ( DataOutput out ) throws IOException {
        out.writeInt(i);
        out.writeInt(j);
    }

    public void readFields ( DataInput in ) throws IOException {
        i = in.readInt();
        j = in.readInt();
    }

    @Override
    public int compareTo ( Pair o ) {
        //return (i == o.i) ? j−o.j : i−o.i;

        if (i == o.i){
            return j - o.j;
        }else{
            return i - o.i;
        }
    }

    @Override
    public String toString () {
        return ""+i+"\t"+j;
    }
}

public class Add extends Configured implements Tool {
    final static int rows = Block.rows;
    final static int columns = Block.columns;

    public static class MapperM extends Mapper<Object, Text, Pair, Triple>{
        @Override
        public void map (Object key, Text value, Context context)
        throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            int i = s.nextInt();
            int j = s.nextInt();
            double v = s.nextDouble();
            Triple t = new Triple(i%rows, j%columns, v);
            Pair p = new Pair(i/rows, j/columns);
            context.write(p, t);
            s.close();
            
        }
    }
    public static class ReducerM extends Reducer<Pair, Triple, Pair, Block>{
        Block block_M = new Block();
        @Override
        public void reduce(Pair key, Iterable<Triple> values, Context context)
        throws IOException, InterruptedException {
            for(Triple t: values){
                block_M.data[t.i][t.j] = t.value;
            }
            context.write(key, block_M);
        }
    }
    public static class MapperN extends Mapper<Object, Text, Pair, Triple>{
        @Override
        public void map (Object key, Text value, Context context)
        throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            int i = s.nextInt();
            int j = s.nextInt();
            double v = s.nextDouble();
            Triple t = new Triple(i%rows, j%columns, v);
            Pair p = new Pair(i/rows, j/columns);
            context.write(p, t);
            s.close();
        }
    }

    public static class ReducerN extends Reducer<Pair, Triple, Pair, Block>{
        Block block_N = new Block();        
        @Override
        public void reduce(Pair key, Iterable<Triple> values, Context context)
        throws IOException, InterruptedException {
            for(Triple t: values){
                block_N.data[t.i][t.j] = t.value;
            }
            context.write(key, block_N);
        }
    }
   
    public static class MapperOne extends Mapper<Pair, Block, Pair, Block>{
        @Override
        public void map (Pair key, Block value, Context context)
        throws IOException, InterruptedException {
            context.write(key, value);            
        }
    }

    public static class MapperTwo extends Mapper<Pair, Block, Pair, Block>{
        @Override
        public void map (Pair key, Block value, Context context)
        throws IOException, InterruptedException {
            context.write(key, value);            
        }
    }

    public static class ReducerMN extends Reducer<Pair, Block, Pair, Block>{
        Block s = new Block();
        @Override
        public void reduce (Pair key, Iterable<Block> values, Context context)
        throws IOException, InterruptedException{
            for(Block b: values){
                for (int i = 0; i < rows ; i++){
                    for (int j = 0; j<columns; j++){
                        s.data[i][j] += b.data[i][j];                    }
                }
            }
            context.write(key, s);
        }
    }

    @Override
    public int run ( String[] args ) throws Exception {
        Job JobM = Job.getInstance();
        JobM.setJobName("JobM");
        JobM.setJarByClass(Add.class);
        JobM.setOutputKeyClass(Pair.class);
        JobM.setOutputValueClass(Block.class);
        JobM.setMapOutputKeyClass(Pair.class);
        JobM.setMapOutputValueClass(Triple.class);
        JobM.setMapperClass(MapperM.class);
        JobM.setReducerClass(ReducerM.class);
        JobM.setInputFormatClass(TextInputFormat.class);
        JobM.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(JobM, new Path(args[0]));
        FileOutputFormat.setOutputPath(JobM, new Path(args[2]));
        //System.out.print(args[2]);
        JobM.waitForCompletion(true);

        Job JobN = Job.getInstance();
        JobN.setJobName("JobN");
        JobN.setJarByClass(Add.class);
        JobN.setOutputKeyClass(Pair.class);
        JobN.setOutputValueClass(Block.class);
        JobN.setMapOutputKeyClass(Pair.class);
        JobN.setMapOutputValueClass(Triple.class);
        JobN.setMapperClass(MapperN.class);
        JobN.setReducerClass(ReducerN.class);
        JobN.setInputFormatClass(TextInputFormat.class);
        JobN.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(JobN, new Path(args[1]));
        FileOutputFormat.setOutputPath(JobN, new Path(args[3]));
        JobN.waitForCompletion(true);

        Job JobMN = Job.getInstance();
        JobMN.setJobName("JobMN");
        JobMN.setJarByClass(Add.class);
        JobMN.setOutputKeyClass(Pair.class);
        JobMN.setOutputValueClass(Block.class);
        JobMN.setMapOutputKeyClass(Pair.class);
        JobMN.setMapOutputValueClass(Block.class);
        JobMN.setReducerClass(ReducerMN.class);
        JobMN.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(JobMN,new Path(args[4]));
        MultipleInputs.addInputPath(JobMN,new Path(args[2]),SequenceFileInputFormat.class,MapperOne.class);
        MultipleInputs.addInputPath(JobMN,new Path(args[3]),SequenceFileInputFormat.class,MapperTwo.class);
        JobMN.waitForCompletion(true); 

        /* ... */
        return 0;
    }

    public static void main ( String[] args ) throws Exception {

        ToolRunner.run(new Configuration(),new Add(),args);
    }
}
