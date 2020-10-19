import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestKmeans {
//    private static final org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(TestKmeans.class);

    @Test
    public void testDistance(){

        String t = "2015/01/06,大里,PM2.5,59,40,34,25,27,29,26,33,42,47,38,24,14,8,17,30,51,62,68,83,83,96,103,110";

        String p1String = "2015/01/10,大里,PM2.5,38,33,31,24,20,19,22,31,31,45,48,49,38,39,43,46,43,36,33,29,37,34,39,33";
        String p2String = "2015/01/15,大里,PM2.5,2,3,7,3,7,5,10,7,13,16,14,8,5,13,20,30,30,33,28,29,33,26,23,12";

        DayInfo d1 = new DayInfo(p1String);
        DayInfo d2 = new DayInfo(p2String);
        DayInfo tt = new DayInfo(t);

        System.out.println("==========================================");
        System.out.println(d1);
        System.out.println(d2);
//        System.out.println("Distance="+r);
        System.out.println("Distance1="+d1.getDistance(tt));
        System.out.println("Distance2="+d2.getDistance(tt));
        System.out.println("==========================================");
    }


    @Test
    public void testCalcNearPoint(){

        String p1String = "2015/01/17,大里,PM2.5,42,33,25,18,13,9,12,20,28,33,33,43,52,55,57,60,66,77,76,76,77,74,82,75";
        String p2String = "2015/01/18,大里,PM2.5,77,66,61,62,70,71,69,71,75,82,90,94,88,75,57,47,33,31,25,21,16,16,18,16";
        String p3String = "2015/01/09,大里,PM2.5,35,34,37,30,25,25,22,21,18,20,14,12,21,31,44,46,52,44,39,37,43,43,42,39";
        String p4String = "2015/01/13,大里,PM2.5,36,36,32,33,38,45,38,45,45,76,84,96,92,87,64,33,21,22,20,15,7,12,9,11";
        String p5String = "2015/01/19,大里,PM2.5,42,33,25,18,13,9,12,20,28,33,33,43,52,55,57,60,66,77,76,76,77,74,82,75";
        String p6String = "2015/01/18,大里,PM2.5,77,66,61,62,70,71,69,71,75,82,90,94,88,75,57,47,33,31,25,21,16,16,18,16";
        String p7String = "2015/01/04,大里,PM2.5,60,56,53,43,53,53,52,44,44,50,49,51,45,42,40,38,36,43,51,63,68,72,66,58";



        DayInfo d1 = new DayInfo(p1String);
        DayInfo d2 = new DayInfo(p2String);

        List<String> k = new ArrayList<>();
        k.add("2015/01/13");
        k.add("2015/01/09");

        List<DayInfo> all = new ArrayList<>();
        all.add(new DayInfo(p1String));
        all.add(new DayInfo(p2String));
        all.add(new DayInfo(p3String));
        all.add(new DayInfo(p4String));
        all.add(new DayInfo(p5String));
        all.add(new DayInfo(p6String));
        all.add(new DayInfo(p7String));

        String p = PM25.Map.calcNearKeyPoint(k,all, new DayInfo(p1String));


        System.out.println("==========================================");
//        System.out.println(d1);
        System.out.println("old="+p1String);
        System.out.println("p="+p);
        System.out.println("==========================================");
    }

    @Test
    public void testCalcAverage(){

        String p1String = "2015/01/17,大里,PM2.5,42,33,25,18,13,9,12,20,28,33,33,43,52,55,57,60,66,77,76,76,77,74,82,75";
        String p2String = "2015/01/18,大里,PM2.5,77,66,61,62,70,71,69,71,75,82,90,94,88,75,57,47,33,31,25,21,16,16,18,16";
        String p3String = "2015/01/09,大里,PM2.5,35,34,37,30,25,25,22,21,18,20,14,12,21,31,44,46,52,44,39,37,43,43,42,39";
        String p4String = "2015/01/13,大里,PM2.5,36,36,32,33,38,45,38,45,45,76,84,96,92,87,64,33,21,22,20,15,7,12,9,11";
        String p5String = "2015/01/03,大里,PM2.5,48,48,43,38,37,36,37,34,37,46,64,77,83,75,68,69,64,65,59,66,71,66,57,48";
        String p6String = "2015/01/18,大里,PM2.5,77,66,61,62,70,71,69,71,75,82,90,94,88,75,57,47,33,31,25,21,16,16,18,16";
        String p7String = "2015/01/04,大里,PM2.5,60,56,53,43,53,53,52,44,44,50,49,51,45,42,40,38,36,43,51,63,68,72,66,58";



        DayInfo d1 = new DayInfo(p1String);

        List<DayInfo> all = new ArrayList<>();
        all.add(new DayInfo(p1String));
        all.add(new DayInfo(p2String));
        all.add(new DayInfo(p3String));
        all.add(new DayInfo(p4String));
        all.add(new DayInfo(p5String));
        all.add(new DayInfo(p6String));
        all.add(new DayInfo(p7String));


        double t = 0;
        double r = 0;
        for (DayInfo dayInfo : all){
            double d = dayInfo.getDistance(d1);
            System.out.println(dayInfo.getDate()+":"+d);
            t += d;

        }
        r = t / all.size();


        System.out.println("==========================================");
//        System.out.println(d1);
        System.out.println("t="+t);
        System.out.println("r="+r);
        System.out.println("==========================================");
    }

    @Test
    public void testCalcDayPoint(){
        String p1String = "2015/01/01,大里,PM2.5,60,56,53,43,53,53,52,44,44,50,49,51,45,42,40,38,36,43,51,63,68,72,66,58";
        String p2String = "2015/01/02,大里,PM2.5,21,22,26,23,20,18,15,21,21,25,29,32,34,29,32,39,51,51,47,43,43,48,47,53";
        String p3String = "2015/01/03,大里,PM2.5,48,48,43,38,37,36,37,34,37,46,64,77,83,75,68,69,64,65,59,66,71,66,57,48";
        String p4String = "2015/01/04,大里,PM2.5,60,56,53,43,53,53,52,44,44,50,49,51,45,42,40,38,36,43,51,63,68,72,66,58";
        String p5String = "2015/01/05,大里,PM2.5,48,42,42,34,34,28,34,35,45,47,54,46,35,19,16,21,24,28,37,52,60,62,64,61";
        String p6String = "2015/01/06,大里,PM2.5,59,40,34,25,27,29,26,33,42,47,38,24,14,8,17,30,51,62,68,83,83,96,103,110";
        String p7String = "2015/01/07,大里,PM2.5,117,110,97,68,47,39,34,27,22,15,14,0,23,18,16,12,10,6,5,9,15,21,23,15";
        String p8String = "2015/01/08,大里,PM2.5,7,9,13,18,11,12,17,29,34,39,41,46,46,44,43,39,41,46,47,48,47,47,43,33";
        String p9String = "2015/01/09,大里,PM2.5,35,34,37,30,25,25,22,21,18,20,14,12,21,31,44,46,52,44,39,37,43,43,42,39";
        String p10String = "2015/01/10,大里,PM2.5,38,33,31,24,20,19,22,31,31,45,48,49,38,39,43,46,43,36,33,29,37,34,39,33";
        String p11String = "2015/01/11,大里,PM2.5,37,41,43,43,27,22,26,34,39,37,51,53,61,56,48,43,37,43,43,48,54,51,46,35";
        String p12String = "2015/01/12,大里,PM2.5,36,40,33,32,33,40,37,34,39,53,60,65,57,50,52,51,43,24,20,28,35,40,30,36";
        String p13String = "2015/01/13,大里,PM2.5,36,36,32,33,38,45,38,45,45,76,84,96,92,87,64,33,21,22,20,15,7,12,9,11";
        String p14String = "2015/01/14,大里,PM2.5,10,7,3,0,3,7,5,1,0,0,0,0,0,0,4,12,13,10,12,14,21,19,15,8";
        String p15String = "2015/01/15,大里,PM2.5,2,3,7,3,7,5,10,7,13,16,14,8,5,13,20,30,30,33,28,29,33,26,23,12";
        String p16String = "2015/01/16,大里,PM2.5,16,15,17,16,16,13,5,10,14,30,30,25,4,22,23,30,33,40,43,45,37,34,38,43";
        String p17String = "2015/01/17,大里,PM2.5,42,33,25,18,13,9,12,20,28,33,33,43,52,55,57,60,66,77,76,76,77,74,82,75";
        String p18String = "2015/01/18,大里,PM2.5,77,66,61,62,70,71,69,71,75,82,90,94,88,75,57,47,33,31,25,21,16,16,18,16";

        List<DayInfo> allDays = new ArrayList<>();
        allDays.add(new DayInfo(p6String));
        allDays.add(new DayInfo(p7String));

//        allDays.add(new DayInfo(p17String));
//        allDays.add(new DayInfo(p10String));
//        allDays.add(new DayInfo(p1String));
//        allDays.add(new DayInfo(p18String));
//        allDays.add(new DayInfo(p5String));
//        allDays.add(new DayInfo(p13String));
//        allDays.add(new DayInfo(p11String));
//        allDays.add(new DayInfo(p12String));
//        allDays.add(new DayInfo(p4String));
//        allDays.add(new DayInfo(p3String));


        List<Double> actual = PM25.Reduce.calcDayPoint(allDays);

        Assert.assertEquals(java.util.Optional.of(88.0).get(),actual.get(0));

        System.out.println("==========================================");
//        System.out.println("DayPoint="+);
        System.out.println("==========================================");
    }

    @Test
    public void testCalcNearDayPoint(){
        String p1String = "2015/01/01,大里,PM2.5,60,56,53,43,53,53,52,44,44,50,49,51,45,42,40,38,36,43,51,63,68,72,66,58";
        String p2String = "2015/01/02,大里,PM2.5,21,22,26,23,20,18,15,21,21,25,29,32,34,29,32,39,51,51,47,43,43,48,47,53";
        String p3String = "2015/01/03,大里,PM2.5,48,48,43,38,37,36,37,34,37,46,64,77,83,75,68,69,64,65,59,66,71,66,57,48";
        String p4String = "2015/01/04,大里,PM2.5,60,56,53,43,53,53,52,44,44,50,49,51,45,42,40,38,36,43,51,63,68,72,66,58";
        String p5String = "2015/01/05,大里,PM2.5,48,42,42,34,34,28,34,35,45,47,54,46,35,19,16,21,24,28,37,52,60,62,64,61";
        String p6String = "2015/01/06,大里,PM2.5,59,40,34,25,27,29,26,33,42,47,38,24,14,8,17,30,51,62,68,83,83,96,103,110";
        String p7String = "2015/01/07,大里,PM2.5,117,110,97,68,47,39,34,27,22,15,14,0,23,18,16,12,10,6,5,9,15,21,23,15";
        String p8String = "2015/01/08,大里,PM2.5,7,9,13,18,11,12,17,29,34,39,41,46,46,44,43,39,41,46,47,48,47,47,43,33";
        String p9String = "2015/01/09,大里,PM2.5,35,34,37,30,25,25,22,21,18,20,14,12,21,31,44,46,52,44,39,37,43,43,42,39";
        String p10String = "2015/01/10,大里,PM2.5,38,33,31,24,20,19,22,31,31,45,48,49,38,39,43,46,43,36,33,29,37,34,39,33";
        String p11String = "2015/01/11,大里,PM2.5,37,41,43,43,27,22,26,34,39,37,51,53,61,56,48,43,37,43,43,48,54,51,46,35";
        String p12String = "2015/01/12,大里,PM2.5,36,40,33,32,33,40,37,34,39,53,60,65,57,50,52,51,43,24,20,28,35,40,30,36";
        String p13String = "2015/01/13,大里,PM2.5,36,36,32,33,38,45,38,45,45,76,84,96,92,87,64,33,21,22,20,15,7,12,9,11";
        String p14String = "2015/01/14,大里,PM2.5,10,7,3,0,3,7,5,1,0,0,0,0,0,0,4,12,13,10,12,14,21,19,15,8";
        String p15String = "2015/01/15,大里,PM2.5,2,3,7,3,7,5,10,7,13,16,14,8,5,13,20,30,30,33,28,29,33,26,23,12";
        String p16String = "2015/01/16,大里,PM2.5,16,15,17,16,16,13,5,10,14,30,30,25,4,22,23,30,33,40,43,45,37,34,38,43";
        String p17String = "2015/01/17,大里,PM2.5,42,33,25,18,13,9,12,20,28,33,33,43,52,55,57,60,66,77,76,76,77,74,82,75";
        String p18String = "2015/01/18,大里,PM2.5,77,66,61,62,70,71,69,71,75,82,90,94,88,75,57,47,33,31,25,21,16,16,18,16";

        List<DayInfo> allDays = new ArrayList<>();
        allDays.add(new DayInfo(p6String));
        allDays.add(new DayInfo(p7String));
        allDays.add(new DayInfo(p17String));
        allDays.add(new DayInfo(p10String));
        allDays.add(new DayInfo(p1String));
        allDays.add(new DayInfo(p18String));
        allDays.add(new DayInfo(p5String));
        allDays.add(new DayInfo(p13String));
        allDays.add(new DayInfo(p11String));
        allDays.add(new DayInfo(p12String));
        allDays.add(new DayInfo(p4String));
        allDays.add(new DayInfo(p3String));

        List<Double> d = new ArrayList<>();
        d.add(77D);
        d.add(5D);
        d.add(22D);
        d.add(78D);
        d.add(16D);
        d.add(18D);
        d.add(79D);
        d.add(9D);
        d.add(43D);
        d.add(31D);
        d.add(33D);
        d.add(98D);
        d.add(67D);
        d.add(57D);
        d.add(89D);
        d.add(23D);
        d.add(89D);
        d.add(4D);
        d.add(19D);
        d.add(98D);
        d.add(67D);
        d.add(90D);
        d.add(32D);
        d.add(23D);


        String actual = PM25.Reduce.calcNearDayPointOfNewKey(allDays);

//        Assert.assertEquals(java.util.Optional.of(88.0).get(),actual.get(0));

        System.out.println("==========================================");
        System.out.println("NearDayPoint=" + actual);
        System.out.println("==========================================");
    }
}
