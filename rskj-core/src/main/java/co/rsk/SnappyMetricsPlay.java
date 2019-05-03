package co.rsk;


import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(description = "Plays an existing Blockchain", name = "play", mixinStandardHelpOptions = true,
        version = "play 1.0")
class SnappyMetricsPlay implements Callable<Void> {


    @CommandLine.Option(names = {"-srcNormal", "--sourceNormal"}, description = "Directory with the Blockchain instance.")
    private String normalBlockchainDir = "/home/julian/.rsk/mainnet-test";

    @CommandLine.Option(names = {"-srcSnappy", "--sourceSnappy"}, description = "Directory with the Snappy Blockchain instance.")
    private String snappyBlockchainDir = "/home/julian/.rsk/mainnet-snappy";

    @CommandLine.Option(names = {"-t", "--times"}, description = "Times to run experiment")
    private int times = 100;

    @CommandLine.Option(names = {"-sp", "--snappy"}, description = "Use snappy or not")
    private boolean useSnappy = false;

    @CommandLine.Option(names = {"-rw", "--readwrite"}, description = "Read/Write (true/false)")
    private boolean rW = true;

    public static void main(String[] args){
        CommandLine.call(new SnappyMetricsPlay(), args);
        System.exit(0);
    }

    @Override
    public Void call() {
        SnappyMetrics sMetrics = new SnappyMetrics(new RskContext(new String[0]), snappyBlockchainDir, normalBlockchainDir, useSnappy, rW);
        sMetrics.runExperiment(times);
        System.gc();
        return null;
    }

}