/* eslint-disable max-len */
/* eslint-disable no-multi-str */

import {exec} from 'child_process';
import {parentPort, workerData, isMainThread} from 'node:worker_threads';
import {fileURLToPath} from 'url';

import MetricSvc from './metricSvc';

class PsMetricSvc extends MetricSvc {
  constructor(parentPort, workerData) {
    super(parentPort, workerData);
    this.psCmd = `ps -A -a -o \'%cpu command\' \
                  | grep \'postgres: ${this.appConfig.clusterName}:\' | grep -v grep \
                  | awk \'{cpu += $1;} END {print cpu;}\'`;
    this.execDelay = 1000;
    this.run();
  }

  readLine() {
    return new Promise((resolve, reject) => {
      setTimeout(()=>{
        exec(this.psCmd, (err, stdout, stderr) => {
          if (err) {
            console.error(`error in executing command ${err}`);
            reject(err);
            return;
          }
          if (stderr) {
            console.error(`command ran into errors: ${stderr}`);
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
    const cpuStat = output.trim();

    return [{
      'graphId': 5,
      'seriesId': 0,
      'ts': timestamp,
      'val': cpuStat,
    }];
  }
}

if (!isMainThread) {
  new PsMetricSvc(parentPort, workerData);
}

export default () => {
  return fileURLToPath(import.meta.url);
};
