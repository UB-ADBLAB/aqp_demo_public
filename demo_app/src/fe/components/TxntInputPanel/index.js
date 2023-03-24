/* eslint-disable max-len */
import React, {useState, useEffect, useRef, Fragment, useMemo, useContext} from 'react';

import Highcharts from 'highcharts';
import HighchartsReact from 'highcharts-react-official';
import Col from 'react-bootstrap/Col';
import Row from 'react-bootstrap/Row';
import Button from 'react-bootstrap/Button';
import Table from 'react-bootstrap/Table';
import Dropdown from 'react-bootstrap/Dropdown';
import * as env from '../../env.js';
import Form from 'react-bootstrap/Form';
import {Container} from 'react-bootstrap/';

let length;
let dataVals = [];
const colors = Highcharts.getOptions().colors;

function TxnInputPanel({clientConfig, cachedQueryAndTxnData}) {
  const sampleConfigObject = {'displayText': '', 'txnConfig': []};
  const [chartData, setChartData] = useState(null);
  const [isTxnInputDisabled, setisTxnInputDisabled] = useState(false);
  const chartRef = useRef(null);
  const [selectedConfig, setSelectedConfig] = useState(sampleConfigObject);
  const [formInputData, setFormInputDataImpl] = useState({
    nThreads: 0,
    nSamplingThreads: 10,
    sampleSize: 10000,
  });

  function setFormInputData(inputData) {
      let obj = {...formInputData};
      if (inputData.nThreads != undefined) {
        if (inputData.nThreads === '') {
            obj.nThreads = '';
        } else {
            let v = parseInt(inputData.nThreads);
            if (!isNaN(v)) {
                obj.nThreads = v;
            }
        }
      }

      if (inputData.nSamplingThreads != undefined) {
        if (inputData.nSamplingThreads === '') {
            obj.nSamplingThreads = '';
        } else {
            let v = parseInt(inputData.nSamplingThreads);
            if (!isNaN(v)) {
                obj.nSamplingThreads = v;
            }
        }
      }
      if (inputData.sampleSize != undefined) {
        if (inputData.sampleSize === '') {
            obj.sampleSize = '';
        } else {
            let v = parseInt(inputData.sampleSize);
            if (!isNaN(v)) {
                obj.sampleSize = v;
            }
        }
      }
      setFormInputDataImpl(obj);
  }

  function setChartConfiguration(config) {
      let totalY = 0;
      let data = [];
      length = parseInt(config.length);
      data = config.map((item, index) => {
        let y;
        if (!item.Probability && item.Probability != 0) {
          y = 0;
        } else {
          y = item.Probability;
        }
        y = y > 100 ? 100 : y;
        totalY += y;
        if (totalY > 100) {
          totalY -= y;
          y = 0;
        }
        return {
          name: item.TxnName,
          y: y,
        };
      });
      // Add UnassignedTransactions object to the config to account for total Txn
      // probability % difference from 100
      data.push({
        name: 'Unassigned',
        y: totalY >= 100 ? 0 : (100 - totalY),
        color: 'transparent',
        showInLegend: false,
      });

      if (dataVals.length < length) {
        data.map((item) => dataVals.push(item.y));
      }
      setChartData(data);
  };

  useEffect(() => {
    if (cachedQueryAndTxnData && cachedQueryAndTxnData.txnMix) {
        setChartConfiguration(cachedQueryAndTxnData.txnMix);
        setFormInputData(cachedQueryAndTxnData);
    }
  }, [cachedQueryAndTxnData]);

  function getTotalPercentage() {
    let total = dataVals.reduce((acc, value) => acc + value, 0);
    total -= dataVals[dataVals.length - 1]; // Removing the unassigned % from total
    return total;
  };
  
  function adjustProbabilityValues(obj) {
    const id = parseInt(obj.id);
    const total = getTotalPercentage();

    if ((total - dataVals[id] + parseInt(obj.value)) <= 100 ) {
      dataVals[id] = parseInt(obj.value);
      dataVals[dataVals.length - 1] = (100 - getTotalPercentage());
    }

    const updatedData = chartData.map((dataPoint, index) =>
      ({...dataPoint, y: parseInt(dataVals[index])}),
    );
    
    setChartData(updatedData);
    const validData = updatedData.filter((data) => {
      return data.name !== 'Unassigned';
    }).map((item) => {
      return {
        "TxnName": item.name,
        "Probability": item.y
      }
    })
    // setQueryAndTxnState({...queryAndTxnState, txnMix: validData});
  };
  
  const handleSliderChange = (event) => {
    const slider = event.target;
    const sliderId = slider.dataset.id;
    const sliderValue = slider.value;

    adjustProbabilityValues({id: sliderId, value: sliderValue});
  };

  const handleTextInputChange = (event) => {
    const probabilityInput = event.target;
    const probabilityInputId = probabilityInput.dataset.id;
    let probabilityInputValue = probabilityInput.value;
    if (probabilityInputValue === '' && probabilityInputValue !== 0) {
      probabilityInputValue = 0;
    }

    adjustProbabilityValues({id: probabilityInputId,
      value: probabilityInputValue});
  };

  const onRun = () => {
    setisTxnInputDisabled(true);

    const validData = chartData.filter((item) => {
      return item.name !== 'Unassigned';
    }).map((item) => {
      return {
        "TxnName": item.name,
        "Probability": item.y
      }
    });

    fetch(env.txnWorkloadRunUrl, {
      method: 'POST',
      body: JSON.stringify({
        txnMix: validData,
        nThreads: formInputData.nThreads,
        nSamplingThreads: formInputData.nSamplingThreads,
        sampleSize: formInputData.sampleSize,
      }),
      headers: {
        'Content-Type': 'application/json',
      },
    })
        .then((response) => response.json())
        .catch((err) => {
          console.error(`error while executing transaction workload , ${err}`);
          setisTxnInputDisabled(false);
        });
  };

  const onStop = () => {
    setisTxnInputDisabled(false);
    fetch(env.txnWorkloadStopUrl, {
      method: 'POST',
      body: JSON.stringify({}),
      headers: {
        'Content-Type': 'application/json',
      },
    })
        .then((response) => response.json())
        .catch((err) => {
          console.error(`error while stopping transaction workload , ${err}`);
        });
  };

  const onReset = () => {
    dataVals = [];
    setisTxnInputDisabled(false);
    setSelectedConfig(selectedConfig);
    if (selectedConfig.txnConfig) {
        setChartConfiguration(selectedConfig.txnConfig);
    }
    setFormInputData({})
  };

  const onConfigSelectionChange = (option) => {
    dataVals = [];
    option = JSON.parse(option);
    setSelectedConfig(option);
    if (option.txnConfig) {
        setChartConfiguration(option.txnConfig);
        const {
                nThreads,
                nSamplingThreads,
                sampleSize
              } = option;
        setFormInputData({nThreads, nSamplingThreads, sampleSize});
    }
  };

  const handleFormInputChange = (event) => {
    let obj = {};
    obj[event.target.id] = event.target.value;
    setFormInputData(obj);
  };

  const renderRangeBox = () => {
    return (
      <Fragment>
        <div>
          <Table borderless id="rangeBox" className="mx-auto">
            <thead>
              <tr>
                <th style={{fontSize: '20px', width: '20%'}}>Txn Name</th>
                <th style={{fontSize: '20px'}}>Probability Input</th>
                <th style={{fontSize: '20px'}}>Probability</th>
              </tr>
            </thead>
            <tbody>
              {chartData.map(({name, y}, i) => (
                i < (chartData.length - 1) &&
                (<tr key={i}>
                  <td contentEditable={false}style={{color: colors[i]}}>
                    {name}
                  </td>
                  <td>
                    <input
                      type="range"
                      id={`slider-id-${i}`}
                      data-id={i}
                      className="range"
                      step="1"
                      min="0"
                      max="100"
                      value={y}
                      onChange={handleSliderChange}
                      style={{width: '100%'}}
                      disabled={isTxnInputDisabled}
                    />
                  </td>
                  <td>
                    <input
                      type="number"
                      id={`slider-value-${i}`}
                      data-id={i}
                      value={y.toString() || '0'}
                      style={{width: '25%', border: 'none'}}
                      min="0"
                      max="100"
                      onChange={handleTextInputChange}
                      disabled={isTxnInputDisabled}
                    />
                    %
                  </td>
                </tr>)
              ))}
            </tbody>
          </Table>
        </div>
      </Fragment>
    );
  };

  return (
    <div className="container-fluid px-1 py-1 my-1 border border-1">
      <Row className='px-1 py-1'>
        <Col xs={7}>
          <Row>
            <Form>
              <Container fluid className='d-flex flex-column h-100 mx-1 my-1'>
                <Row className="align-items-end">
                  <Form.Group as={Col} controlId='txnMixDropDown' className='px-1 py-1'>
                    <Form.Label>Select Config</Form.Label>
                    <Dropdown onSelect={onConfigSelectionChange}>
                      <Dropdown.Toggle variant='secondary' disabled={isTxnInputDisabled}>
                        {selectedConfig.displayText !== '' ? selectedConfig.displayText : 'Select Config'}
                      </Dropdown.Toggle>
                      <Dropdown.Menu>
                        {
                          clientConfig.txnPanelSelectionConfig && clientConfig.txnPanelSelectionConfig.map((option, index) => {
                            return (
                              <Dropdown.Item key={`option-${index}`}
                                eventKey={JSON.stringify(option)}
                                active={selectedConfig.displayText === option.displayText}>
                                {option.displayText}
                              </Dropdown.Item>
                            );
                          })
                        }
                      </Dropdown.Menu>
                    </Dropdown>
                  </Form.Group>
                  <Form.Group as={Col} controlId='nThreads' className='px-1 py-1'>
                    <Form.Label># Threads</Form.Label>
                    <Form.Control type="number"
                      disabled={isTxnInputDisabled}
                      onChange={handleFormInputChange}
                      value={formInputData.nThreads}/>
                  </Form.Group>
                  <Form.Group as={Col} controlId='nSamplingThreads' className='px-1 py-1'>
                    <Form.Label># Sampling Threads</Form.Label>
                    <Form.Control type="number"
                      disabled={isTxnInputDisabled}
                      onChange={handleFormInputChange}
                      value={formInputData.nSamplingThreads}/>
                  </Form.Group>
                  <Form.Group as={Col} controlId='sampleSize' className='px-1 py-1'>
                    <Form.Label>Sample Size</Form.Label>
                    <Form.Control type="number"
                      disabled={isTxnInputDisabled}
                      onChange={handleFormInputChange}
                      value={formInputData.sampleSize}/>
                  </Form.Group>
                </Row>
              </Container>
            </Form>
          </Row>
          <Row>
            {chartData && renderRangeBox()}
          </Row>
          <Row>
            <div className="d-flex align-items-baseline">
              <Button variant="success" className='mx-1 my-1'
                onClick={onRun} disabled={isTxnInputDisabled}>RUN</Button>
              <Button variant="danger" className='mx-1 my-1'
                onClick={onStop}>STOP</Button>
              <Button variant="primary" className='mx-1 my-1'
                onClick={onReset} disabled={isTxnInputDisabled}>
                  RESET</Button>
            </div>
          </Row>
        </Col>
        <Col xs={5}>
          <HighchartsReact
            highcharts={Highcharts}
            options={{
              chart: {
                type: 'pie',
                plotBackgroundColor: null,
                plotBorderWidth: null,
                plotShadow: false,
              },
              title: {
                text: 'TPC-C Transactions',
                style: {
                  fontFamily: 'system-ui',
                  fontWeight: 'bold',
                  fontSize: '20px',
                },
                align: 'center',
                y: 20,
              },
              tooltip: {
                pointFormat: '{series.TxnName}: <b>{point.y}%</b>',
                percentageDecimals: 1,
              },
              plotOptions: {
                pie: {
                  allowPointSelect: true,
                  cursor: 'pointer',
                  dataLabels: {
                    enabled: false,
                  },
                  showInLegend: true,
                },
              },
              accessibility: {
                enabled: false,
              },
              series: [
                {
                  type: 'pie',
                  dataType: 'json',
                  name: 'Probability',
                  data: chartData,
                },
              ],
            }}
            ref={chartRef}
          />
        </Col>
      </Row>
    </div>
  );
}

export default TxnInputPanel;
