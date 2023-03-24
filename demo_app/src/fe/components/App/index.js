import React, {useEffect, useState} from 'react';

import QueryPanel from '../QueryPanel';
import TxnInputPanel from '../TxntInputPanel';
import PerformanceMonitor from '../PerformanceMonitor';

import Container from 'react-bootstrap/Container';
import Navbar from 'react-bootstrap/Navbar';
import Tab from 'react-bootstrap/Tab';
import Tabs from 'react-bootstrap/Tabs';

import * as env from '../../env.js';

import '../../styles.css';
import 'bootstrap/dist/css/bootstrap.min.css';

const AppTitle = `Approximate Queries over Concurrent Updates using Index-Assisted Sampling`;
const QueryPanelTitle = `Ad-hoc Queries`;
const TxnPanelTitle = `Background Workload`;
const PerformanceDashboardPanelTitle = `Performance Metrics`;

const connToAQPSocket = new Promise((resolve, reject) => {
  // eslint-disable-next-line max-len
  const aqpSocket = window.aqpSocket = window.aqpSocket || new WebSocket(env.webSocketAddress);
  aqpSocket.onopen = (e) => {
    resolve();
  };
});

const getClientConfig = new Promise((resolve, reject) => {
  fetch(env.clientConfigUrl, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
  })
      .then((response) => response.json())
      .then((data) => {
        resolve(data);
      })
      .catch((err) => {
        console.error(`error while fetching client configuration workload , ${err}`);
      });
});


const App = () => {
  const [key, setKey] = useState('queryPanel');
  const [clientConfigState, setClientConfigState] = useState({});
  const [cachedQueryAndTxnData, setCachedQueryAndTxnData] = useState({});

  useEffect(() => {
    connToAQPSocket.then(() => {
      console.log(`websocket connection established`);
    }).then(() => {
      return getClientConfig;
    }).then((data) => {
      console.log(`got client configuration`);
      setClientConfigState(data.clientConfig);
      setCachedQueryAndTxnData(data.aqpQueryAndTxnState);
    });
  }, []);

  return (
    <div className='container-fluid'>
      <Navbar bg="dark" variant="dark">
        <Container>
          <Navbar.Brand className="mx-auto navbar-brand-title">
            {' '}
            {AppTitle}
          </Navbar.Brand>
        </Container>
      </Navbar>
      <Tabs
        id="controlled-tab"
        activeKey={key}
        onSelect={(k) => setKey(k)}
        className="mb-3"
        style={{paddingTop: '1rem'}}
      >
        <Tab eventKey="queryPanel"
          title={QueryPanelTitle}
          className='custom-tab-title'>
          <QueryPanel clientConfig={clientConfigState} cachedQueryAndTxnData={cachedQueryAndTxnData} />
        </Tab>
        <Tab eventKey="txPanel"
          title={TxnPanelTitle}
          className='custom-tab-title'>
          <TxnInputPanel clientConfig={clientConfigState} cachedQueryAndTxnData={cachedQueryAndTxnData} />
        </Tab>
        <Tab eventKey="performanceDashboardPanel"
          title={PerformanceDashboardPanelTitle}
          className='custom-tab-title'>
          <PerformanceMonitor />
        </Tab>
      </Tabs>
    </div>
  );
};

export default App;
