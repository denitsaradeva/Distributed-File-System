public class Controller implements Runnable {

    private int cPort;
    private int RFactor;
    private long timeout;
    private long rebalancePeriod;

    public Controller(int cPort, int RFactor, long timeout, long rebalancePeriod) {
        this.cPort = cPort;
        this.RFactor = RFactor;
        this.timeout = timeout;
        this.rebalancePeriod = rebalancePeriod;
    }


    @Override
    public void run() {

    }
}
