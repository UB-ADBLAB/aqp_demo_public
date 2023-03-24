/* eslint-disable max-len */
/* eslint-disable no-multi-str */

import {exec} from 'child_process';
import {parentPort, workerData, isMainThread} from 'node:worker_threads';
import {fileURLToPath} from 'url';

import MetricSvc from './metricSvc';

class IOStatMetricSvc extends MetricSvc {
  constructor(parentPort, workerData) {
    super(parentPort, workerData);
    this.ioStatCmd = `iotop -b -n 1 -k | grep \'postgres: ${this.appConfig.clusterName}:\' \
                      | grep -v grep \
                      | awk \'BEGIN{read=0; write=0;}{read += $4; write +=$6} \
                      END{print read, write;} \'`;
    this.execDelay = 1000;
    this.run();
  }

  readLine() {
    return new Promise((resolve, reject) => {
      setTimeout(()=>{
        exec(this.ioStatCmd, (err, stdout, stderr) => {
          if (err) {
            console.error(`error in executing iostat command ${err}`);
            reject(err);
            return;
          }
          if (stderr) {
            console.error(`iostat command ran into errors: ${stderr}`);
            reject(err);
            return;
          }
          this.timestamp = Date.now();
          resolve({output: stdout});
        });
      }, this.execDelay);
    });
  }

  parseLine(output) {
    const [read, write] = output.trim().split(' ');
    return [
      {"graphId": 3, "seriesId": 0, "ts": this.timestamp, "val": read ? read / 1024 : 0},
      {"graphId": 4, "seriesId": 0, "ts": this.timestamp, "val": write ? write / 1024 : 0},
    ];
  }
}

if (!isMainThread) {
  new IOStatMetricSvc(parentPort, workerData);
}

export default () => {
  return fileURLToPath(import.meta.url);
};
