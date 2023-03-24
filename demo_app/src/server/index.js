/* eslint-disable max-len */
/* eslint-disable no-multi-str */
import express from 'express';
import {execAdhocQuery} from './app';
import cors from 'cors';
import {spawn} from 'child_process';
import {WebSocketServer} from 'ws';
import {Worker} from 'node:worker_threads';
import {readFileSync, writeFileSync} from 'fs';
import path from 'path';
import {fileURLToPath} from 'url';
import { existsSync } from 'fs';

import psMetricSvcFilePath from './svc/psSvc';
import nativeMetricSvcFilePath from './svc/nativeSvc';
import ioStatMetricSvcFilePath from './svc/iostatSvc';
import memMetricSvcFilePath from './svc/memSvc';
import {setAppConfig, getAppConfig} from './env.js';

const app = express();

app.use(express.static('public'));
app.use(express.json());
app.use(cors());

let javaProcess = null;
const svcFilePaths = [];
const svcWorkers = [];

svcFilePaths.push(psMetricSvcFilePath());
svcFilePaths.push(nativeMetricSvcFilePath());
svcFilePaths.push(ioStatMetricSvcFilePath());
svcFilePaths.push(memMetricSvcFilePath());

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const aqpConfigPath = path.resolve(__dirname, '../common/aqpConfig.json');

let aqpConfig;
try {
  aqpConfig = readFileSync(aqpConfigPath);
} catch (error) {
  console.error(`failed to read aqp configuration. cannot start application ${error}`);
  process.exit(-1);
}
setAppConfig(JSON.parse(aqpConfig.toString()));


const aqpQueryAndTxnStatePath = path.resolve(__dirname, '../common/aqpQueryAndTxnState.json');
let aqpQueryAndTxnState = {};
let createFile = false;

try {
  if (existsSync(aqpQueryAndTxnStatePath)) {
    console.log('file exists');
    const queryAndTxn = readFileSync(aqpQueryAndTxnStatePath);
    aqpQueryAndTxnState = JSON.parse(queryAndTxn.toString());
  }
} catch (error) {
  console.error(`failed to check if query and txn state file exists. cannot start application ${error}`);
  process.exit(-1);
}

const server = app.listen(getAppConfig().aqpServerPort, (err) => {
  if (!err) {
    console.log(`AQP application server running on port: ${getAppConfig().aqpServerPort}`);
  }
});

let gClient = null;

svcFilePaths.forEach((filePath) => {
  const worker = new Worker(filePath, {workerData: getAppConfig()});
  svcWorkers.push(worker);
  worker.on('message', (data) => {
    if (gClient) {
      gClient.send(JSON.stringify(data));
    }
  });
});

const wss = new WebSocketServer({server});
wss.on('connection', (client) => {
  console.log('client connected');
  if (gClient) {
    client.close();
  } else {
    gClient = client;
    gClient.on('close', () => {
      if (javaProcess) {
        javaProcess.stdin.write('STOP');
        javaProcess.stdin.end();
        javaProcess.on('exit', () =>{
          console.log('java driver program finished');
        });
        javaProcess = null;
      }
      gClient = null;
      console.log('connection closed');
    });
  }
});

const procExitHandler = () => {
  if (gClient) {
    gClient.close();
    gClient = null;
  }
  svcWorkers.forEach((worker) => {
    worker.postMessage({type: 'CLOSE'});
  });
  svcWorkers.length = 0;

  // dump previous query/txn configuration and results
  try {
    writeFileSync(aqpQueryAndTxnStatePath, JSON.stringify(aqpQueryAndTxnState));
  } catch (error) {
    console.error(`failed to write query and txn state into file ${error}`);
  }
  
  process.exit(0);
};
process.on('SIGINT', procExitHandler);
process.on('SIGTERM', procExitHandler);

const invokeJavaDriver = (txnMix) => {
  return new Promise((resolve, reject) => {
    const txns = txnMix.map((txn) => { return txn.TxnName; }).toString();
    // Probability is in percentages
    const percents = txnMix.map((txn) => { return txn.Probability; }).toString();
    const appConfig = getAppConfig();

    const txnDriverJar = path.resolve(__dirname, appConfig.txnDriverJar);

    javaProcess = spawn('java', ['-jar', appConfig.txnDriverJar,
                                 aqpQueryAndTxnState.nThreads,
                                 aqpQueryAndTxnState.nSamplingThreads,
                                 appConfig.host,
                                 appConfig.port,
                                 appConfig.database,
                                 appConfig.username,
                                 appConfig.password,
                                 txns,
                                 percents,
                                 aqpQueryAndTxnState.sampleSize]);
    javaProcess.stdout.on('data', (data) => {
      console.log(`${data}`);
      if (data.toString().includes('PIDS')) {
        const opWithPids = data.toString().split(/n/)[0];
        if (opWithPids) {
          const pIds = opWithPids.trim().split(' ');
          pIds.shift();
          resolve(pIds);
        }
      }
    });
    javaProcess.stderr.on('data', (data) => {
      reject(data);
    });
  });
};

app.post('/txnWorkloadRun', (req, res) => {
  const {body} = req;
  const {txnMix, nThreads, nSamplingThreads, sampleSize} = body;

  if (!txnMix || txnMix.length == 0) {
    const err = `invalid input for transaction or percent array`;
    console.error(err);
    res.end(err);
    return;
  }

  if (javaProcess) {
    const err = `driver program already running. cannot instantitate another`;
    console.error(err);
    res.end(err);
    return;
  }

  aqpQueryAndTxnState.txnMix = txnMix;
  aqpQueryAndTxnState.nThreads = nThreads;
  aqpQueryAndTxnState.nSamplingThreads = nSamplingThreads;
  aqpQueryAndTxnState.sampleSize = sampleSize;

  invokeJavaDriver(txnMix)
      .then((data) => { // inform worker threads the pids to monitor
        const appConfig = getAppConfig();
        svcWorkers.forEach((worker) => {
          worker.postMessage({type: 'RUN', PIDS_TO_MONITOR: data, appConfig});
        });
      })
      .catch((err) => {
        console.error(`invocation of java driver program failed with error ${err}`);
      });

  res.end(JSON.stringify('executing transaction work load'));
});

app.post('/txnWorkloadStop', (req, res) => {
  if (javaProcess) {
    javaProcess.stdin.write('STOP');
    javaProcess.stdin.end();
    javaProcess.on('exit', () =>{
      console.log('java driver program finished');
    });
    javaProcess = null;
  }

  svcWorkers.forEach((worker) => {
    worker.postMessage({type: 'STOP'});
  });

  res.end(JSON.stringify('stopped driver program'));
});

app.post('/adhocQuery', (req, res) => {
  const {body} = req;
  const {queryAndIdx} = body;
  const {
    query, 
    queryIndex,
  } = queryAndIdx;

  aqpQueryAndTxnState = {...aqpQueryAndTxnState, query, queryIndex};
  if (typeof aqpQueryAndTxnState.queries === 'undefined') {
    aqpQueryAndTxnState.queries = [{}, {}, {}];
  }
  aqpQueryAndTxnState.queries[queryIndex] = {};
  aqpQueryAndTxnState.queries[queryIndex].lastQuery = query;

  const queryStart = Date.now();
  execAdhocQuery(query)
      .then((result) => {
        const queryDuration = Date.now() - queryStart;
        const response = {};
        const cols = [];

        res.setHeader('content-type', 'application/json');
        if (result) {
          const {rows, rowCount, fields} = result;

          if (rowCount > 0) {
            fields.forEach((field) => cols.push(field.name));
            response.data = rows;
            response.count = rowCount;
            response.cols = cols;
          }

          // we write the successful query execution to disk
          aqpQueryAndTxnState.queries[queryIndex].response = response;
          aqpQueryAndTxnState.queries[queryIndex].queryDuration = queryDuration;

          res.send(JSON.stringify({...response, queryDuration}));
        } else {
          res.send(JSON.stringify({...response, queryDuration}));
        }
      });
});

app.post('/getClientConfig', (req, res) => {
  const clientConfigPath = path.resolve(__dirname, '../common/clientConfig.json');
  let clientConfig = null;

  try {
    const fileContent = readFileSync(clientConfigPath);
    clientConfig = JSON.parse(fileContent.toString());
  } catch (error) {
    console.error(`failed to read client configuration. ${error}`);
    res.status(500).send('Internal server error');
    return;
  }

  let response = {clientConfig: clientConfig,
                  aqpQueryAndTxnState: aqpQueryAndTxnState};
  res.setHeader('content-type', 'application/json');
  res.send(JSON.stringify(response));
});
