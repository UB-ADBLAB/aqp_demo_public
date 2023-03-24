/* eslint-disable max-len */
/* eslint-disable no-multi-str */

import {exec} from 'child_process';
import {parentPort, workerData, isMainThread} from 'node:worker_threads';
import {fileURLToPath} from 'url';

import MetricSvc from './metricSvc';

class MemMetricSvc extends MetricSvc {
  constructor(parentPort, workerData) {
    super(parentPort, workerData);
    this.memCmd = `${this.appConfig.sMemPath} -c \'uss pss command\' \
                    | grep \'postgres: ${this.appConfig.clusterName}:\' \
                    | grep -v grep \
                    | awk \'{uss += $1; pss += $2} END{print uss, pss}\'`;
    this.execDelay = 1000;
    this.run();
  }

  readLine() {
    return new Promise((resolve, reject) => {
      setTimeout(()=>{
        exec(this.memCmd, (err, stdout, stderr) => {
          if (err) {
            console.error(`error in executing smem command ${err}`);
            reject(err);
            return;
          }
          if (stderr) {
            console.error(`smem command ran into errors: ${stderr}`);
            reject(err);
            return;
          }
          resolve({output: stdout});
        });
      }, this.execDelay);
    });
  }

  parseLine(output) {
    const timestamp = Date.now();
    const [uss, pss] = output.trim().split(' ');
    return [{
      'graphId': 6,
      'seriesId': 0,
      'ts': timestamp,
      'val': uss ? uss / 1024 : 0,
    },
    {
      'graphId': 6,
      'seriesId': 1,
      'ts': timestamp,
      'val': pss ? pss / 1024 : 0,
    }];
  }
}

if (!isMainThread) {
  new MemMetricSvc(parentPort, workerData);
}

export default () => {
  return fileURLToPath(import.meta.url);
};
