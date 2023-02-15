import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Vertex implements Writable {
    public short tag;           // 0 for a graph vertex, 1 for a group number
    public long group;          // the group where this vertex belongs to
    public long VID;            // the vertex ID
    //public long[] adjacent;
    public Vector<Long> adjacent = new Vector<Long>(); 

    Vertex() {}

    Vertex(short t, long g, long v, Vector<Long> a){
        tag = t;
        group = g;
        VID = v;
        adjacent = a;
    }


    Vertex(short t, long g){
        tag = t;
        group = g;
    }

    public void write( DataOutput out ) throws IOException{
        out.writeShort(tag);
        out.writeLong(group);
        out.writeLong(VID);
        out.writeInt(adjacent.size());

        for (int i = 0; i < adjacent.size(); i++){
            out.writeLong(adjacent.get(i));
        }
    }

    public void readFields( DataInput in ) throws IOException{
        tag = in.readShort();
        group = in.readLong();
        VID = in.readLong();
        Vector<Long> adj = new Vector();
        int s = in.readInt();
        for(int i = 0; i < s; i++ ){
            adj.addElement(in.readLong());
        }
        adjacent = adj;
    }

    @Override
    public String toString(){
        return ""+tag+"\t"+group+"\t"+VID+"\t"+adjacent;    
    }

    /* ... */
}

public class Graph {

    public static class FirstMapper extends Mapper<Object, Text, LongWritable, Vertex>{
        @Override
        public void map (Object key, Text value, Context context)
        throws IOException, InterruptedException{
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            Vector<Long> adjnew = new Vector();
            long vid = s.nextLong();
            while(s.hasNextLong())
            {
                adjnew.addElement(s.nextLong());
            }        
            context.write(new LongWritable(vid), new Vertex((short)0, vid, vid, adjnew));
            s.close();
        }
    }

    public static class SecondMapper extends Mapper<LongWritable, Vertex, LongWritable, Vertex>{
        @Override
        public void map (LongWritable key, Vertex value, Context context)
        throws IOException, InterruptedException{
            context.write(new LongWritable(value.VID), value); //graph topology

            for (Long n: value.adjacent){
                context.write(new LongWritable(n), new Vertex((short)1, value.group)); //group # to adjacent vertex
            }
        }
    }

    public static class SecondReducer extends Reducer<LongWritable, Vertex, LongWritable, Vertex>{
        @Override
        public void reduce(LongWritable vid, Iterable<Vertex> values, Context context)
        throws IOException, InterruptedException{
            Vector<Long> adj = new Vector();
            long m = Long.MAX_VALUE;
            for(Vertex v: values){
                if (v.tag == 0){
                    adj = v.adjacent;
                }
                m = Math.min(m, v.group);
            }
            context.write(new LongWritable(m), new Vertex((short)0, m, vid.get(), adj));
        }
    }

    public static class FinalMapper extends Mapper<LongWritable, Vertex, LongWritable, LongWritable>{
        @Override
        public void map(LongWritable group, Vertex value, Context context) 
        throws IOException, InterruptedException{
            context.write(group, new LongWritable(1));
        }
    }

    public static class FinalReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable>{
        @Override
        public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException{
            long sum = 0;
            for(LongWritable v : values){
                sum += v.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    /* ... */

    public static void main ( String[] args ) throws Exception {
        Job FirstJob = Job.getInstance();
        FirstJob.setJobName("FirstJob");
        FirstJob.setJarByClass(Graph.class);
        FirstJob.setMapperClass(FirstMapper.class);
        FirstJob.setOutputKeyClass(LongWritable.class);
        FirstJob.setOutputValueClass(Vertex.class);
        FirstJob.setMapOutputKeyClass(LongWritable.class);
        FirstJob.setMapOutputValueClass(Vertex.class);
        FirstJob.setInputFormatClass(TextInputFormat.class);
        FirstJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(FirstJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(FirstJob, new Path(args[1] + "/f0"));
        /* ... First Map-Reduce job to read the graph */
        FirstJob.waitForCompletion(true);
        for ( short i = 0; i < 5; i++ ) {
            Job SecondJob = Job.getInstance();
            SecondJob.setJobName("SecondJob");
            SecondJob.setJarByClass(Graph.class);
            SecondJob.setMapperClass(SecondMapper.class);
            SecondJob.setReducerClass(SecondReducer.class);
            SecondJob.setOutputKeyClass(LongWritable.class);
            SecondJob.setOutputValueClass(Vertex.class);
            SecondJob.setMapOutputKeyClass(LongWritable.class);
            SecondJob.setMapOutputValueClass(Vertex.class);
            SecondJob.setInputFormatClass(SequenceFileInputFormat.class);
            SecondJob.setOutputFormatClass(SequenceFileOutputFormat.class);
            FileInputFormat.setInputPaths(SecondJob, new Path(args[1]+"/f"+i));
            FileOutputFormat.setOutputPath(SecondJob, new Path(args[1]+"/f"+(i+1)));
            /* ... Second Map-Reduce job to propagate the group number */
            SecondJob.waitForCompletion(true);
        }
        Job FinalJob = Job.getInstance();
        FinalJob.setJobName("FinalJob");
        FinalJob.setJarByClass(Graph.class);
        FinalJob.setMapperClass(FinalMapper.class);
        FinalJob.setReducerClass(FinalReducer.class);
        FinalJob.setOutputKeyClass(LongWritable.class);
        FinalJob.setOutputValueClass(LongWritable.class);
        FinalJob.setMapOutputKeyClass(LongWritable.class);
        FinalJob.setMapOutputValueClass(LongWritable.class);
        FinalJob.setInputFormatClass(SequenceFileInputFormat.class);
        FinalJob.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(FinalJob, new Path(args[1] + "/f5"));
        FileOutputFormat.setOutputPath(FinalJob, new Path(args[2]));
        /* ... Final Map-Reduce job to calculate the connected component sizes */
        FinalJob.waitForCompletion(true);
    }
}
