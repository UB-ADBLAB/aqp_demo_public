/* eslint-disable max-len */
/* eslint-disable no-multi-str */

/**
 * metric service is an abstract class with abstract methods that should
 * be overriden in child metric service classes.
 */
class MetricSvc {
  constructor(parentPort, workerData) {
    if (new.target === MetricSvc) {
      throw new Error('cannot instantiate metric service as its\
            abstract class.');
    }

    this.parentPort = parentPort;
    this.appConfig = workerData;
    this.PIDS_TO_MONITOR = null;

    this.parentPort.on('message', (msgData) => {
      const {type} = msgData;
      if (type && type == 'RUN') {
        const {PIDS_TO_MONITOR} = msgData;
        this.PIDS_TO_MONITOR = PIDS_TO_MONITOR;
      }
      if (type && type == 'STOP') {
        this.stop();
      }
      if (type && type == 'CLOSE') {
        this.close();
      }
    });
  }

  readLine() {
    throw new Error('readLine needs to be overriden in child class');
  }

  parseLine() {
    throw new Error('parseLine needs to be overriden in child class');
  }

  // final method. child cannot override
  async run() {
    while (true) {
      try {
        const {output, err} = await this.readLine();
        if (!output) { // we dont have a valid output. something like null is returned.
          continue;
        }
        if (err) {
          break;
        }
        const result = this.parseLine(output);
        if (result && result.length != 0) {
          this.parentPort.postMessage(result);
        }
      } catch (error) {
        console.error(`encountered error while executing metric service : ${error}`);
      }
    }
  }

  stop() {
    this.PIDS_TO_MONITOR = null;
  }

  close() {
    this.parentPort.close();
    process.exit(0);
  }
}
Object.defineProperty(MetricSvc.prototype, 'run', {writable: false});

export default MetricSvc;
