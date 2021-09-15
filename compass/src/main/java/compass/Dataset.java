package compass;

import java.util.*;
import tech.tablesaw.api.*;
import tech.tablesaw.io.csv.*;
import java.io.*;
import com.google.common.base.Joiner;
import java.security.MessageDigest;
import java.io.*;
import java.util.*;
import java.nio.file.*;
import java.util.stream.*;
import static java.util.stream.Collectors.toList;

public class Dataset{
    List<String> columnNames;
    String path;
    List<ColumnType> columnTypes;
    char separator;
    boolean preloaded = false;
    Map<String, Integer> rowMap = null;
    Map<String, Double> lowerBounds;
    Table allRows = null;
    Map<String, Double> upperBounds;
    static String hashColumn = "__tuplehash__";
    public Dataset(){}
    public static String getHashColumnName(){
      return hashColumn;
    }

    public String toString(){
      return this.getClass().getName();
    }
    public double getLowerBound(String a){
      return lowerBounds.get(a);
    }
    public double getUpperBound(String a){
      return upperBounds.get(a);
    }

    public void preload(){
      System.out.println("Preloading " + path);
      rowMap = new HashMap<String ,Integer>();
      try(Stream<String> stream = Files.lines(Paths.get(path))){
          List<String> lines = stream.collect(toList());
          System.out.println("Parsing");
          allRows = CSVStringsToTableWithoutHeader(lines);
          int idx = 0;
          System.out.println("Building rowmap");
          for(Row row: allRows){
            rowMap.put(row.getString(hashColumn), idx);
            idx++;
          }
      }catch(IOException e){
        e.printStackTrace();
          System.out.println(e);
      }

      System.out.println("Loaded  " + rowMap.size() + " rows");
      preloaded = true;
    }

    private void addHashColumnIfNotExist(){
      if(!columnNames.contains(hashColumn)){
        columnNames.add(hashColumn);
        columnTypes.add(ColumnType.STRING);
      }
    }

    public Dataset(List<String> cNames, List<ColumnType> cTypes, char c) {
      columnNames = cNames;
      columnNames.add(hashColumn);
      columnTypes = cTypes;
      columnTypes.add(ColumnType.STRING);
      separator = c;
    }

    public static String getStringHash(String s){
      //adding 0x is to avoid table saw automatically trying to parse it as double.
      return "0x"+org.apache.commons.codec.digest
              .DigestUtils.sha256Hex(s).substring(0, 16);
    }

    public Table CSVStringsToTableWithoutHeader(List<String> dataCache) throws IOException{
      dataCache.add(0, Joiner.on(",").join(columnNames));
      return this.CSVStringsToTable(dataCache);
    }

    public Table fetchRowMap(List<String> dataCache){
      Table ret = null;
        for(int i = 0; i<dataCache.size(); i++){
          String s = dataCache.get(i);
          if (s.contains(hashColumn)) continue;
          String hash = getStringHash(s);
          if(ret == null){
            ret = allRows.rows(rowMap.get(hash));
          }else
            ret.addRow(allRows.row(rowMap.get(hash)));
      }
      return ret;
    }

    public Table CSVStringsToTable(List<String> dataCache) throws IOException{
      if(dataCache.size() == 0) return Table.create(this.getClass().getSimpleName());
      Table t = null;
      if(preloaded){
        t = fetchRowMap(dataCache);
        if(t != null) return t;
        // System.out.println("Empty sample: " + dataCache);
      }

      for(int i = 0; i<dataCache.size(); i++){
        String s = dataCache.get(i);
        if (s.contains(hashColumn)) continue;
        dataCache.set(i, s+separator+getStringHash(s));
      }

      t = Table.read().usingOptions(
          CsvReadOptions.builderFromString(Joiner.on(System.lineSeparator()).join(dataCache))
          .separator(separator)
          .columnTypesToDetect(columnTypes));
      return t;
    }

    public static Dataset createDatasetByName(String name, boolean preload){
      String datasetName = name.toLowerCase();
      Dataset ds;
      if (datasetName.equals("intelwireless")){
          ds = new IntelWireless();
      }else if (datasetName.equals("taxi")){
          ds = new NYCTaxi();
      }else if (datasetName.equals("taxipdt")){
          ds = new NYCTaxiPDT();
      }else if (datasetName.equals("instacart")){
          ds = new Instacart();
      }else if (datasetName.equals("etf")){
          ds = new ETF();
      }else{
          throw new IllegalArgumentException("Unsupported dataset " + datasetName);
      }
      ds.addHashColumnIfNotExist();
      if(preload) ds.preload();
      return ds;
    }

}

class Instacart extends Dataset{
  public Instacart(){
    separator = ',';
  }
}

class NYCTaxi extends Dataset{
  public NYCTaxi(){
    separator = ',';
    path = "../data/taxi.csv";
    columnNames = new ArrayList<String>(Arrays.asList(
      "pickup_date","pickup_datetime","dropoff_datetime","pickup_time","dropoff_date","dropoff_time","PULocationID","DOLocationID","trip_distance"));
    columnTypes = new ArrayList<ColumnType>(Arrays.asList(
                                ColumnType.DOUBLE, ColumnType.DOUBLE,
                                ColumnType.DOUBLE, ColumnType.DOUBLE,
                                ColumnType.DOUBLE, ColumnType.DOUBLE,
                                ColumnType.DOUBLE));
    lowerBounds = new HashMap<String, Double>() {{
      put("trip_distance", 0.0);

    }};
    upperBounds = new HashMap<String, Double>() {{
      put("trip_distance", 1000.0);
    }};
  }
}

class NYCTaxiPDT extends Dataset{
  public NYCTaxiPDT(){
    separator = ',';
    path = "../data/taxi_pickupdatetime_sorted_noheader.csv";
    columnNames = new ArrayList<String>(Arrays.asList(
      "pickup_date","pickup_datetime","dropoff_datetime","pickup_time","dropoff_date","dropoff_time","PULocationID","DOLocationID","trip_distance"));
    columnTypes = new ArrayList<ColumnType>(Arrays.asList(
                                ColumnType.DOUBLE, ColumnType.DOUBLE,
                                ColumnType.DOUBLE, ColumnType.DOUBLE,
                                ColumnType.DOUBLE, ColumnType.DOUBLE,
                                ColumnType.DOUBLE));
    lowerBounds = new HashMap<String, Double>() {{
      put("trip_distance", 0.0);

    }};
    upperBounds = new HashMap<String, Double>() {{
      put("trip_distance", 1000.0);
    }};
  }
}

class ETF extends Dataset{
  public ETF(){
    separator = ',';
    path = "../data/etf-noheader.csv";
    columnNames = new ArrayList<String>(Arrays.asList("pk", "open", "high", "low", "close", "volume", "idate"));
    columnTypes = new ArrayList<ColumnType>(Arrays.asList(
                                ColumnType.DOUBLE,
                                ColumnType.DOUBLE, ColumnType.DOUBLE,
                                ColumnType.DOUBLE, ColumnType.DOUBLE,
                                ColumnType.DOUBLE, ColumnType.DOUBLE));
    lowerBounds = new HashMap<String, Double>() {{
      put("volume", 0.0);

    }};
    upperBounds = new HashMap<String, Double>() {{
      put("volume", 1100000000.0);
    }};
  }
}


class IntelWireless extends Dataset{
  public IntelWireless(){
    separator = ',';
    path = "../data/intel.csv";
    columnTypes = new ArrayList<ColumnType>(Arrays.asList(
                                ColumnType.STRING, ColumnType.STRING,
                                ColumnType.STRING, ColumnType.STRING,
                                ColumnType.DOUBLE, ColumnType.DOUBLE, ColumnType.DOUBLE, ColumnType.DOUBLE,
                                ColumnType.DOUBLE, ColumnType.DOUBLE));
    columnNames = new ArrayList<String>(Arrays.asList("date","time","epoch","moteid","temperature","humidity","light","voltage","idate","itime"));
    lowerBounds = new HashMap<String, Double>() {{
      put("voltage", 0.5);
      put("light", 0.0);
    }};
    upperBounds = new HashMap<String, Double>() {{
      put("voltage", 32000.0);
      put("light", 2000.0);
    }};
  }
}

class SmallData extends Dataset{
  public SmallData(){
    columnNames = new ArrayList<String>(Arrays.asList("C", "A"));
    columnTypes = new ArrayList<ColumnType>(Arrays.asList(ColumnType.DOUBLE, ColumnType.DOUBLE));
  }
}