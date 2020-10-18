import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class PM25 {
    /**
     * Map前處理，主要將全部點位指定同一個key值，後續交由 同一台MAP 處理
     */
    public static class PreMap extends Mapper<LongWritable, Text, Text, TextArrayWritable> {

        private final List<String> days = new ArrayList<>();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken();
                days.add(token);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            context.write(new Text("AllDays"), new TextArrayWritable(days.toArray(new String[0])));
        }
    }

    /**
     * Reduce前處理，在這邊進行了隨機分群
     */
    public static class PreReduce extends Reducer<Text, TextArrayWritable, Text, Text> {

        public void reduce(Text key, Iterable<TextArrayWritable> values, Context context)
                throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            int k = Integer.parseInt(conf.get("k"));

            for (TextArrayWritable val : values) {

                int[] keyPointIndexes = randomCommon(0, val.get().length, k);

                List<DayInfo> keyPoints =
                        Arrays.stream(keyPointIndexes)
                                .mapToObj(rIndex -> new DayInfo(val.get()[rIndex].toString()))
                                .collect(Collectors.toList());

                Random r = new Random();
                for (Writable d : val.get()){
                    //隨機分k群
                    context.write(new Text(keyPoints.get(r.nextInt(k)).getDate()), new Text(d.toString()));
                }
            }
        }
    }

    /**
     * 這邊將收到的 day資訊，依上一 round產生的n個質心 KeyDay進行GroupBy分組
     */
    public static class Map extends Mapper<LongWritable, Text, Text, TextArrayWritable> {

        /**
         * KEY值：該分類的key值<br>
         * Value值：該分類的Value 集合<br>
         * example:<br>
         * 2015/07/14:{day.....}<br>
         * 2015/05/10:{day.....}
         */
        private final java.util.Map<String, List<String>> groupDaysByKeyDay = new HashMap<>();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);

            while (tokenizer.hasMoreTokens()) {
                String keyDay = tokenizer.nextToken();
                String dayInfoStr = tokenizer.nextToken();

                System.out.println("pDay="+keyDay);
                System.out.println("dayInfoStr="+dayInfoStr);

                addOrUpdate(groupDaysByKeyDay, keyDay, new DayInfo(dayInfoStr));
            }
        }

        /**
         * 這邊將點位依距離重新進行分配
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            List<DayInfo> allDays = new ArrayList<>();
            List<String> allKeyPoints = new ArrayList<>();
            java.util.Map<String, List<String>> resultMaps = new HashMap<>();

            groupDaysByKeyDay.keySet().forEach(key -> {
                groupDaysByKeyDay.get(key).stream().map(DayInfo::new).forEach(allDays::add);
                allKeyPoints.add(key);
            });

            System.out.println("allDays.size="+allDays.size());
            allDays.stream().map(d -> "DayInfo=" + d.toString()).forEach(System.out::println);
            allKeyPoints.stream().map(k -> "key=" + k).forEach(System.out::println);

            allDays.forEach(dayInfo -> {
                //如果該點位就是質心點位，就不用比較，直接加入自己的集合裡
                if (allKeyPoints.contains(dayInfo.getDate())) {
                    addOrUpdate(resultMaps, dayInfo.getDate(), dayInfo);
                    return;
                }

                String nearPoint = calcNearPoint(allKeyPoints, allDays, dayInfo);
                addOrUpdate(resultMaps, nearPoint, dayInfo);
            });

            for(String key : resultMaps.keySet()){
                String[] values = resultMaps.get(key).toArray(new String[0]);
                System.out.println("resultMaps.key=" + key);
                System.out.println("resultMaps.values=" + Arrays.toString(values));

                context.write(new Text(key), new TextArrayWritable(values));
            }
        }

        private void addOrUpdate(java.util.Map<String, List<String>> resultMaps, String keyDate, DayInfo inputDayInfo) {
            if (!resultMaps.containsKey(keyDate)) {
                List<String> list = new ArrayList<>();
                list.add(inputDayInfo.getSource());

                resultMaps.put(keyDate, list);
            } else {
                List<String> dayInfoStrs = resultMaps.get(keyDate);
                dayInfoStrs.add(inputDayInfo.getSource());

                resultMaps.put(keyDate, dayInfoStrs);
            }
        }

        /**
         * 計算p點更靠近哪個keyPoint,並回傳KeyPoint
         * @param allKeyPoints
         * @param allDays
         * @param p
         * @return
         */
        public static String calcNearPoint(List<String> allKeyPoints,List<DayInfo> allDays, DayInfo p){

            List<Double> distances =
                    allKeyPoints.stream()
                            .map(keyPoint -> allDays.stream()
                                    .filter(x -> x.getDate().equals(keyPoint))
                                    .findFirst().get())
                            .map(p::getDistance)
                            .collect(Collectors.toList());

            double min = distances.stream().filter(y-> y >0).mapToDouble(Double::doubleValue).min().getAsDouble();

            return allKeyPoints.toArray()[distances.indexOf(min)].toString();
        }
    }

    /**
     * 重新計算質心
     */
    public static class Reduce extends Reducer<Text, TextArrayWritable, Text, Text> {

        public void reduce(Text key, Iterable<TextArrayWritable> values, Context context)
                throws IOException, InterruptedException {

            List<DayInfo> thisReduceAllDays = new ArrayList<>();

            for (TextArrayWritable val : values) {
                Arrays.stream(val.get())
                        .map(s -> new DayInfo(s.toString()))
                        .forEach(thisReduceAllDays::add);
            }

            System.out.println("======================Reduce Start======================");
            System.out.println("Reduce.Key="+key);
            System.out.println("Reduce.allDays.size="+thisReduceAllDays.size());
            thisReduceAllDays.stream().map(d -> "DayInfo=" + d.toString()).forEach(System.out::println);

            DayInfo keyInfo = thisReduceAllDays.stream()
                    .filter(x -> x.getDate().equals(key.toString()))
                    .findAny().get();
            System.out.println("Reduce.keyInfo="+keyInfo);

            double avgDistance = calcAvgDistance(thisReduceAllDays, keyInfo);

            String newKey = calcNearAvgDistanceOfDay(thisReduceAllDays, avgDistance, keyInfo);
            System.out.println("Reduce.newKey="+newKey);

            for (DayInfo d : thisReduceAllDays){
                context.write(new Text(newKey), new Text(d.getSource()));
            }
        }

        public static double calcAvgDistance(List<DayInfo> thisReduceAllDays, DayInfo keyInfo) {
            double total = thisReduceAllDays.stream().mapToDouble(dayInfo -> dayInfo.getDistance(keyInfo)).sum();

            return total / thisReduceAllDays.size();
        }

        /**
         * 計算最靠近平均距離的點
         * @param thisReduceAllDays
         * @param avgDistance
         * @param keyInfo
         * @return
         */
        public static String calcNearAvgDistanceOfDay(List<DayInfo> thisReduceAllDays, double avgDistance, DayInfo keyInfo){

            List<Double> distances = thisReduceAllDays.stream()
                    .map(d -> (Math.abs(avgDistance - d.getDistance(keyInfo))))
                    .collect(Collectors.toList());

            //只有自己的距離，就回傳自己
            if (distances.size() == 1) {
                return keyInfo.getDate();
            }

            double min = distances.stream()
                    .filter(y -> y > 0)
                    .mapToDouble(Double::doubleValue)
                    .min().getAsDouble();

            return ((DayInfo) thisReduceAllDays.toArray()[distances.indexOf(min)]).getDate();
        }

    }
    public static class TextArrayWritable extends ArrayWritable {

        public TextArrayWritable() {
            super(Text.class);
        }
        public TextArrayWritable(String[] strings) {
            super(Text.class);
            Text[] texts = new Text[strings.length];
            for (int i = 0; i < strings.length; i++  ) {
                texts[i] = new Text(strings[i]);
            }
            set(texts);
        }
    }

    private static int[] randomCommon(int min, int max, int n){
        if (n > (max - min + 1) || max < min) {
            return new int[]{};
        }
        int[] result = new int[n];
        int count = 0;
        while(count < n) {
            int num = (int) (Math.random() * (max - min)) + min;
            boolean flag = true;
            for (int j = 0; j < n; j++) {
                if(num == result[j]){
                    flag = false;
                    break;
                }
            }
            if(flag){
                result[count] = num;
                count++;
            }
        }
        return result;
    }

    public static void main(String[] args) throws Exception {
        Job job = getInitJob(args);

        job.waitForCompletion(true);

        //hadoop jar output.jar PM25 pm25_m 2 50 t115 r115
        int iter = Integer.parseInt(args[2]);
        String inputPathBase = args[3];
        String resultPathBase = args[4];

        for (int i = 0; i < iter; i++) {
            String inputPath = inputPathBase + "/" + i;
            String outputPath = (i == iter - 1)
                    ? resultPathBase
                    : inputPathBase + "/" + (i + 1);

            getJob("PM25_byNick_" + i, inputPath, outputPath)
                    .waitForCompletion(true);
        }
    }

    private static Job getJob(String jobName,String inputPath,String outputPath) throws IOException {

        //Job Configuration
        Job job = Job.getInstance(new Configuration(), jobName);

        //set output key and value type
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TextArrayWritable.class);

        //set Map and Reduce Function
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setJarByClass(PM25.class);

        //set input and output Format
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //set input and output Locations
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job;
    }

    private static Job getInitJob(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.set("k", args[1]);

        //Job Configuration
        Job job = Job.getInstance(conf, "PM25_Init_byNick");

        //set output key and value type
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TextArrayWritable.class);

        //set Map and Reduce Function
        job.setMapperClass(PreMap.class);
        job.setReducerClass(PreReduce.class);
        job.setJarByClass(PM25.class);

        //set input and output Format
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //set input and output Locations
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]+"/0"));
        return job;
    }
}
