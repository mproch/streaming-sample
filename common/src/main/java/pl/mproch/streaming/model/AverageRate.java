package pl.mproch.streaming.model;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AverageRate {
    private long rateSum;
    private int count;

    public AverageRate add(int rate) {
        return new AverageRate(rateSum + rate, count + 1);
    }

    public double computeRate() {
        return rateSum / count;
    }
}