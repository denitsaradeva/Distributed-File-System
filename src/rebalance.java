import java.util.ArrayList;

public class rebalance {
    public static void main(String[] args) {
        ArrayList<String> firstD = new ArrayList<>();
        ArrayList<String> secondD = new ArrayList<>();
        ArrayList<String> thirdD = new ArrayList<>();
        ArrayList<ArrayList<String>> all = new ArrayList<>();
        all.add(firstD);
        all.add(secondD);
        all.add(thirdD);

        ArrayList<String> files = new ArrayList<>();
        files.add("aa.txt");
        files.add("bb.txt");
        files.add("cc.txt");
        files.add("dd.txt");
        files.add("ee.txt");
        files.add("ff.txt");
        files.add("gg.txt");
        files.add("hh.txt");

        firstD.add("aa.txt");
        secondD.add("bb.txt");
        secondD.add("cc.txt");
        secondD.add("dd.txt");
        secondD.add("ee.txt");
        secondD.add("ff.txt");
        secondD.add("gg.txt");
        secondD.add("hh.txt");
        System.out.println(all.get(1).get(0));

        int N = all.size();
        int F = 6;
        int R = 2;
        int lowerBound = (int) Math.floor((R * F * 1.0) / (N * 1.0));
        int upperBound = (int) Math.ceil((R * F * 1.0) / (N * 1.0));

        int countReplTimes;

        for (String file : files) {
            countReplTimes = 0;
            for (ArrayList<String> dStore : all) {
                if (dStore.contains(file)) {
                    countReplTimes++;
                }
            }
            if (countReplTimes < R) {
                //TODO
            } else if (countReplTimes > R) {
                //TODO
            }
        }

        for (ArrayList<String> dStore : all) {
            if (dStore.size() < lowerBound) {
                int freeSpace = lowerBound - dStore.size();
            } else if (dStore.size() > upperBound) {
                int countDel = dStore.size() - upperBound;

            }
        }





    }
}
