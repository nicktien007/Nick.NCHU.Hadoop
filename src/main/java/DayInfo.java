import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DayInfo {
    private String id;
    private String date;
    private String city;
    private String dayType;
    private List<Integer> values = new ArrayList<>();
    private String source;

    @Override
    public String toString() {
        return "DayInfo{" +
                "id='" + id + '\'' +
                ", date='" + date + '\'' +
                ", city='" + city + '\'' +
                ", dayType='" + dayType + '\'' +
                ", values=" + values +
                '}';
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public DayInfo(String dayString) {

        this.source = dayString;
        String[] dsSplit = dayString.split(",");

        this.date = dsSplit[0];
        this.city = dsSplit[1];
        this.dayType = dsSplit[2];
        this.id = dsSplit[0].replace("/", "");


        this.values.addAll(
                Arrays.stream(dsSplit)
                        .filter(x -> Arrays.asList(dsSplit).indexOf(x) > 2)
                        .mapToInt(Integer::parseInt)
                        .collect(
                                ArrayList::new,
                                ArrayList::add,
                                ArrayList::addAll)
        );
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getDayType() {
        return dayType;
    }

    public void setDayType(String dayType) {
        this.dayType = dayType;
    }

    public List<Integer> getValues() {
        return values;
    }

    public void setValues(List<Integer> values) {
        this.values = values;
    }

    public double getDistance(DayInfo targetDay) {
        List<Integer> d1Values = this.values;
        List<Integer> d2Values = targetDay.getValues();
        int total = 0;

        for (int i = 0; i < (long) d1Values.size(); i++) {
            int d1v = d1Values.get(i);
            int d2v = d2Values.get(i);

            total += (d1v - d2v) * (d1v - d2v);
        }

        return Math.sqrt(total);
    }
}
