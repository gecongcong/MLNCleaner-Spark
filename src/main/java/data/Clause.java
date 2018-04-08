package data;

import java.io.Serializable;

/**
 * Created by gcc on 18-3-14.
 */
public class Clause implements Serializable{
    String content = "";
    String weight = "";
    int number = 0;

    public Clause(String content, String weight, int number) {
        this.content = content;
        this.weight = weight;
        this.number = number;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getWeight() {
        return weight;
    }

    public void setWeight(String weight) {
        this.weight = weight;
    }

    public int getNumber() {
        return number;
    }

    public void setNumber(int number) {
        this.number = number;
    }
}
