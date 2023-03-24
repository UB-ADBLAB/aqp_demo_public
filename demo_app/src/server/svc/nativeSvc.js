/* eslint-disable max-len */
/* eslint-disable no-multi-str */

import {parentPort, workerData, isMainThread} from 'node:worker_threads';
import {fileURLToPath} from 'url';
import net from 'node:net';

import MetricSvc from './metricSvc';

const RETRY_COUNT = 10;
const AQP_NATIVE_SLEEP_DURATION = 1000; // milliseconds
const SLEEP_BURST_TIME = 10; // milliseconds

class NativeMetricSvc extends MetricSvc {
  constructor(parentPort, workerData) {
    super(parentPort, workerData);

    this.nativeConn = net.createConnection({host: this.appConfig.host, port: this.appConfig.aqpNativeSvcPort}, () => {
      console.log('native connection established');
      // we set the result even though data is not returned yet, so that readLine
      // can start waiting at regular intervals.
      this.resultSetAt = Date.now();
      this.target = this.resultSetAt + this.sleepDuration;
      this.run();
    });

    this.nativeConn.on('data', (data) => {
      // parse the result to synchronize the "result set at" with native driver.
      if (data) {
        const output = data.toString().split('\n');
        let lastItemIdx = output.length - 1;
        let metric;
        let metricFound = false;

        // we find the correct latest metric
        while (lastItemIdx >= 0) {
          metric = output[lastItemIdx];
          if (metric && metric[0] == '[' && metric[metric.length - 1] == ']') {
            metricFound = true;
            break;
          }
          lastItemIdx--;
        }

        if (metricFound) {
          this.previousMetricData = this.metricData;
          this.metricData = JSON.parse(metric);
          this.resultSetAt = this.metricData[0] / 1000; // timestamp - micro to milli
          this.bDataPresent = true;
          this.target = this.resultSetAt + this.sleepDuration;
        }
      }
    });

    this.nativeConn.on('error', (e) => {
      console.error(`native connection failed ${e && e.message}`);
    });

    this.sleepDuration = AQP_NATIVE_SLEEP_DURATION;
    this.previousMetricData = null;
    this.metricData = null;
    this.resultSetAt = null;
    this.bDataPresent = false;
    this.target = null;
  }

  sleep(ms) {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        resolve();
      }, ms);
    });
  }

  async readLine() {
    let data;

    if (this.bDataPresent) {
      data = this.metricData;
      this.bDataPresent = false;
      return {output: data};
    }

    let currentTime = Date.now();
    const deltaTime = this.target - currentTime;

    if (deltaTime > 0) {
      await this.sleep(deltaTime);
    }

    for (let i = RETRY_COUNT; i >= 0; --i) {
      if (this.bDataPresent) { // we have the data
        data = this.metricData;
        // this.target = this.resultSetAt + this.sleepDuration;
        this.bDataPresent = false;
        return {output: data};
      }
      if (i > 0) {
        await this.sleep(SLEEP_BURST_TIME);
      }
    }

    currentTime = Date.now();
    this.resultSetAt += Math.floor((currentTime - this.resultSetAt) / this.sleepDuration) * this.sleepDuration;
    this.target = this.resultSetAt + this.sleepDuration;
    return {output: null};
  }

  parseLine(output) {
    if (this.previousMetricData == null) {
      return null;
    }
    const [previousTimeStamp, previousInserts, previousSamplesAccepted, previousSamplesRejected] = this.previousMetricData;
    const [timeStamp, nInserts, nSamplesAccepted, nSamplesRejected] = output;

    const deltaTime = (timeStamp - previousTimeStamp) / 1000000.0;
    const avgTimeStamp = (timeStamp + previousTimeStamp) / 2000.;

    const insertionThroughput = (nInserts - previousInserts) / deltaTime;
    const nSamplesTotal = (nSamplesAccepted + nSamplesRejected - previousSamplesAccepted - previousSamplesRejected);
    const samplingThroughput = nSamplesTotal / deltaTime;
    const rejectionRate = (nSamplesTotal == 0) ? 0 : ((nSamplesRejected - previousSamplesRejected) * 100.0 / nSamplesTotal);

    return [
        {"graphId": 0, "seriesId": 0, "ts": avgTimeStamp, "val": insertionThroughput},
        {"graphId": 1, "seriesId": 0, "ts": avgTimeStamp, "val": samplingThroughput},
        {"graphId": 2, "seriesId": 0, "ts": avgTimeStamp, "val": rejectionRate}
    ];
  }

  close() {
    this.nativeConn.end();
    super.close();
  }
}

if (!isMainThread) {
  new NativeMetricSvc(parentPort, workerData);
}

export default () => {
  return fileURLToPath(import.meta.url);
};
