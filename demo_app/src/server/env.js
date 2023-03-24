import path from 'path';
import {fileURLToPath} from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

/* eslint-disable max-len */
let appConfig = null;

export const setAppConfig = (config) => {
    appConfig = config;
    appConfig.txnDriverJar = path.resolve(__dirname + "/../common", appConfig.txnDriverJar);
};

export const getAppConfig = () => {
  return appConfig;
};

