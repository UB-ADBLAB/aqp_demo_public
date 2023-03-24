/* eslint-disable max-len */
import React, {
  useEffect,
  Fragment,
  useState,
  useRef,
} from 'react';

import Graph from './Graph';

import Col from 'react-bootstrap/Col';
import Row from 'react-bootstrap/Row';
import Container from 'react-bootstrap/Container';
import Form from 'react-bootstrap/Form';

const PerformanceMonitor = () => {
  const graphConfig = [
    {
      graphId: 0,
      graphTitle: 'AB-tree insertion throughput',
      seriesKeys: ['#inserts / second'],
    },
    {
      graphId: 1,
      graphTitle: 'AB-tree sampling throughput',
      seriesKeys: ['#samples / second'],
    },
    {
      graphId: 2,
      graphTitle: 'AB-tree rejection rate',
      seriesKeys: ['%'],
    },
    {
      graphId: 3,
      graphTitle: 'Read Bandwidth',
      seriesKeys: ['MB/s'],
    },
    {
      graphId: 4,
      graphTitle: 'Write Bandwidth',
      seriesKeys: ['MB/s'],
    },
    {
      graphId: 5,
      graphTitle: 'CPU Usage',
      seriesKeys: ['%'],
    },
    {
      graphId: 6,
      graphTitle: 'Memory Usage',
      seriesKeys: ['Excluding shared memory (MB)', 'Including shared memory (MB)'],
    },
  ];

  const [point, setPoint] = useState({data: null});
  const [isSyncToggleChecked, setIsSyncToggleChecked] = useState(true);
  const [navigatorEventData, setNavigatorEventData] = useState({});
  const syncToggleRef = useRef();
  syncToggleRef.current = isSyncToggleChecked;
  const aqpSocket = window.aqpSocket;

  useEffect(() => {
    if (aqpSocket) {
      aqpSocket.onmessage = (e) => {
        setPoint((prevPoint) => ({...prevPoint, data: JSON.parse(e.data)}));
      };
    }

    // Code for Testing with random data in setInterval
    // let x;
    // const intervalId = setInterval(function() {
    //   x = (new Date()).getTime();
    //   setPoint((prevPoint) => (
    //     {...prevPoint,
    //       data: [
    //         {'graphId': 0, 'seriesId': 0, 'ts': x, 'val': Math.round(Math.random() * 100)},
    //         {'graphId': 1, 'seriesId': 0, 'ts': x, 'val': Math.round(Math.random() * 100)},
    //         {'graphId': 2, 'seriesId': 0, 'ts': x, 'val': Math.round(Math.random() * 100)},
    //         {'graphId': 3, 'seriesId': 0, 'ts': x, 'val': Math.round(Math.random() * 100)},
    //         {'graphId': 3, 'seriesId': 1, 'ts': x, 'val': Math.round(Math.random() * 100)},
    //         {'graphId': 4, 'seriesId': 0, 'ts': x, 'val': Math.round(Math.random() * 100)},
    //         {'graphId': 4, 'seriesId': 1, 'ts': x, 'val': Math.round(Math.random() * 100)},
    //         {'graphId': 5, 'seriesId': 0, 'ts': x, 'val': Math.round(Math.random() * 100)},
    //         {'graphId': 5, 'seriesId': 1, 'ts': x, 'val': Math.round(Math.random() * 100)},
    //         {'graphId': 6, 'seriesId': 0, 'ts': x, 'val': Math.random()},
    //         {'graphId': 7, 'seriesId': 0, 'ts': x, 'val': Math.round(Math.random() * 100)},
    //         {'graphId': 7, 'seriesId': 1, 'ts': x, 'val': Math.random() * 100},
    //       ],
    //     }));
    // }, 1000);

    // return () => {
    //   clearInterval(intervalId);
    // };
  });

  function handleSyncToggleChange(e) {
    setIsSyncToggleChecked(e.target.checked);
  }

  const handleNavigatorChange = (obj) => {
      if (syncToggleRef.current) {
        setNavigatorEventData(obj);
      }
  };

  return (
    <Fragment>
      <Container fluid className="px-1 py-1 my-1 border border-1">
        <Form.Check
          type="switch"
          id="custom-switch"
          label="Synchronize Navigators"
          reverse
          checked={isSyncToggleChecked}
          onChange={handleSyncToggleChange}
        />
        {
          graphConfig.map((config, index) => {
            if (index%3 === 0) {
              return (
                <Row key={index}>
                  <Col xs={4}>
                    <Graph title={config.graphTitle}
                      data={[]}
                      point={point.data}
                      config={config}
                      onNavigatorChange={handleNavigatorChange}
                      navigatorEventData={navigatorEventData}
                      syncToggleChecked={isSyncToggleChecked}>
                    </Graph>
                  </Col>
                  {
                    graphConfig[index + 1] &&
                                <Col xs={4}>
                                  <Graph title={graphConfig[index + 1].graphTitle} data={[]}
                                    point={point.data}
                                    config={graphConfig[index + 1]}
                                    onNavigatorChange={handleNavigatorChange}
                                    navigatorEventData={navigatorEventData}
                                    syncToggleChecked={isSyncToggleChecked}>
                                  </Graph>
                                </Col>
                  }
                  {
                    graphConfig[index + 2] &&
                                <Col xs={4}>
                                  <Graph title={graphConfig[index + 2].graphTitle} data={[]}
                                    point={point.data}
                                    config={graphConfig[index + 2]}
                                    onNavigatorChange={handleNavigatorChange}
                                    navigatorEventData={navigatorEventData}
                                    syncToggleChecked={isSyncToggleChecked}>
                                  </Graph>
                                </Col>
                  }

                </Row>
              );
            }
          })
        }
      </Container>
    </Fragment>
  );
};

export default PerformanceMonitor;
