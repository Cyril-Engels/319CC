public class Test {
    public static void main(String[] args) {
        String line = "";
        line = line.toLowerCase().replaceAll("[^a-z]", " ").trim().replaceAll(" {2,}", " ");
        if (line.length() != 0) {
            String[] tokens = line.split(" ");
            int length = tokens.length;
            for (int i = 0; i < length; i++) {
                System.out.println(tokens[i]);
            }
            for (int i = 0; i < length - 1; i++) {
                System.out.println(tokens[i] + " " + tokens[i + 1]);
            }
            for (int i = 0; i < length - 2; i++) {
                System.out.println(tokens[i] + " " + tokens[i + 1] + " " + tokens[i + 2]);
            }
            for (int i = 0; i < length - 3; i++) {
                System.out.println(tokens[i] + " " + tokens[i + 1] + " " + tokens[i + 2] + " " + tokens[i + 3]);
            }
            for (int i = 0; i < length - 4; i++) {
                System.out.println(tokens[i] + " " + tokens[i + 1] + " " + tokens[i + 2] + " " + tokens[i + 3] + " " + tokens[i + 4]);
            }
        }
    }


    public static int score(String s) {
        String date1 = "2014-04-20", date2 = "2015-03-20";
        int count1 = 0, count2 = 0, count3 = 0;
        String[] days = s.split(";");
        for (int i = 0; i < days.length; i++) {
            String[] scores = days[i].split(",");
            while (scores[0].compareTo(date1) >= 0 && scores[0].compareTo(date2) <= 0) {
                count1 += Integer.parseInt(scores[1]);
                count2 = Integer.parseInt(scores[2]) > count2 ? Integer.parseInt(scores[2]) : count2;
                count3 = Integer.parseInt(scores[3]) > count2 ? Integer.parseInt(scores[3]) : count3;
            }
        }
        return count1 + count2 + count3;
    }
}