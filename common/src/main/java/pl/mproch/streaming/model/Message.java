package pl.mproch.streaming.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Message {

    private long time;

    private String userId;

    private String text;

    private int rate;

    private User user;

    public Message(long time, String userId, String text, int rate) {
        this.time = time;
        this.userId = userId;
        this.text = text;
        this.rate = rate;
    }

    public Message withUser(User user) {
        return new Message(time, userId, text, rate, user);

    }

}
