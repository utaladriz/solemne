package cl.ucinf.clase;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Created by utaladriz on 16-05-16.
 */

public  class TokenizerMapper 
       extends Mapper<Object, Text, Text, DoubleWritable>{
    
    private DoubleWritable temperatura = new DoubleWritable();
    private Text año = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      //IdEstacion;Nombre Estacion;Latitud;Longitud;Altura;Año;Mes;Dia;TMinima;TMaxima
      StringTokenizer itr = new StringTokenizer(value.toString(),";");
      try {
      itr.nextToken();//IdEstacion
      itr.nextToken(); //Nombre
      itr.nextToken();//Latitud
      itr.nextToken();//Longitud
      itr.nextToken();//Altura
      año.set(itr.nextToken());//Año
      itr.nextToken();//Mes
      itr.nextToken();//Dia
      temperatura.set(Double.parseDouble(itr.nextToken()));//Tminima
      itr.nextToken();//Tmaxima
      context.write(año, temperatura);
      }
      catch (Exception ex){

      }
      
    }
  }