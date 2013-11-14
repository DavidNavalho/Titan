package swift.test.microbenchmark.interfaces;

import swift.test.microbenchmark.RawDataCollector;

public interface MicroBenchmarkWorker extends Runnable {

    void stop();

    ResultHandler getResults();
    
    RawDataCollector getRawData();

    String getWorkerID();

}
