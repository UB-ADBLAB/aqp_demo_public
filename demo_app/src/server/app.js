import pkg from 'pg';
import {getAppConfig} from './env.js';

const {Client} = pkg;

export const execAdhocQuery = (query) => {
  const appConfig = getAppConfig();
  const client = new Client({
    user: appConfig.username,
    password: appConfig.password,
    database: appConfig.database,
    host: appConfig.host,
    port: appConfig.port,
  });

  return client.connect()
      .then(() => client.query({text: query, rowMode: 'array'}))
      .catch((err) => console.error('db connection failed : ', err))
      .finally(() => {
        client.end();
      });
};
