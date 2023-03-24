/* eslint-disable max-len */
import React, {useCallback, useMemo, useState, useContext, useEffect} from 'react';

import ResultsGrid from '../ResultsGrid';

import Form from 'react-bootstrap/Form';
import Button from 'react-bootstrap/Button';
import {Dropdown} from 'react-bootstrap';
import {DropdownButton} from 'react-bootstrap';
import {Row, Col} from 'react-bootstrap';

import ErrorGraph from './ErrorGraph';
import Pagination from '../Pagination';
import * as env from '../../env.js';

const ITEMS_PER_PAGE = 50;
const NUM_ROWS_IN_QUERYBOX = 13;
const PLOT_GRAPH = 'Plot relative errors';
const CLEAR_GRAPH = 'Clear relative errors';

function QueryPanel({clientConfig, cachedQueryAndTxnData}) {
  const [queries, setQueries] = useState(Array(3).fill(''));
  const [queryInProgress, setQueryInProgress] = useState(Array(3).fill(false));
  const [queryResult, setQueryResult] = useState(Array(3).fill({}));
  const [itemsToShow, setItemsToShow] = useState(Array(3).fill([]));
  const [selectedQuery, setSelectedQuery] = useState('Select query');
  const [plotGraph, setPlotGraph] = useState(false);
  const [showErrorGraph, setShowErrorGraph] = useState(false);

  const numItems = queryResult.count ? queryResult.count : 0;

  const handleQueryChange = (index) => (e) => {
    e.preventDefault();
    const newQueries = [...queries];
    newQueries[index] = e.target.value;
    setQueries(newQueries);
  };

  const onQueryClear = (index) => (e) => {
    e.preventDefault();
    const newItemsToShow = [...itemsToShow];
    newItemsToShow[index] = [];
    setItemsToShow(newItemsToShow);
  };

  const onRun = (index) => (e) => {
    const query = queries[index];
    if(query.lenght == 0) {
      console.error('invalid query');
      return;
    }
    const queryAndIdx = {queryIndex: index, query};
    fetch(env.adhocQueryUrl, {
      method: 'POST',
      body: JSON.stringify({
        // query: queries[index],
        queryAndIdx
      }),
      headers: {
        'Content-Type': 'application/json',
      },
    })
        .then((response) => response.json())
        .then((response) => {
          const {data} = response;
          
          const newItems = data.slice(0, 50);
          itemsToShow[index] = newItems;
          setItemsToShow(itemsToShow);
          
          queryResult[index] = response;
          setQueryResult(queryResult);
        })
        .catch((err) => {
          console.error(`error while executing query , ${err}`);
        })
        .finally(() => {
          const newStatus = [...queryInProgress];
          newStatus[index] = false;
          setQueryInProgress(newStatus);
        });

    const newStatus = [...queryInProgress];
    newStatus[index] = true;
    setQueryInProgress(newStatus);
  };

  const onPageChange = useCallback((event) => {
    const startOffset = (event.selected * ITEMS_PER_PAGE) % numItems;
    const endOffset = startOffset + ITEMS_PER_PAGE;
    const dataToShow = queryResult.data.slice(startOffset, endOffset);
    setItemsToShow(dataToShow);
  }, [queryResult]);

  const onDropDownChange = (index) => {
    const {
      displayText,
      sqlStatements
    } = clientConfig.queryPanelSelectionConfig[index];
    setQueries(sqlStatements);
    setSelectedQuery(displayText);
  };

  useEffect(() => {
    if (!cachedQueryAndTxnData || !cachedQueryAndTxnData.queries) {
      return ;
    }

    let cachedQueries = cachedQueryAndTxnData.queries;
    let n = cachedQueries.length;
    if (n > 3) {
      n = 3;
    }
    
    let itemsToShowNew = [[], [], []];
    let queriesToSet = ["", "", ""];
    let responsesToSet = [{}, {}, {}];
    for (let i = 0; i < n; ++i) {
        if (!cachedQueries[i].lastQuery) {
            continue;
        }
        queriesToSet[i] = cachedQueries[i].lastQuery;
        if (!cachedQueries[i].response) {
          itemsToShowNew[i] = [];
          responsesToSet[i] = {};
        } else {
          itemsToShowNew[i] = cachedQueries[i].response.data.slice(0, 50);
          responsesToSet[i] = cachedQueries[i].response;
          responsesToSet[i].queryDuration = cachedQueries[i].queryDuration;
        }
    }
    setQueries(queriesToSet);
    setQueryResult(responsesToSet);
    setItemsToShow(itemsToShowNew);
  }, [cachedQueryAndTxnData]);

  const reRenderErrorGraph = () => {
    setPlotGraph(!plotGraph);
    setShowErrorGraph(true);
  };

  const toggleErrorGraph = () => {
    setShowErrorGraph(false);
  };

  return (
      <div className="container-fluid px-1 py-1 my-1 border border-1">
          <div className='container-fluid'>
            <Row>
              <Col md="auto">
                <DropdownButton title={selectedQuery} variant='secondary' onSelect={onDropDownChange}>
                  {
                    clientConfig.queryPanelSelectionConfig && clientConfig.queryPanelSelectionConfig.map((selectConfig, index) => {
                      return (
                        <Dropdown.Item eventKey={index} key={index}>{selectConfig.displayText}</Dropdown.Item>
                      );
                    })
                  }
                </DropdownButton>
              </Col>
              <Col md="auto">
                <Button variant="dark" onClick={reRenderErrorGraph}>{PLOT_GRAPH}</Button>
              </Col>
              <Col md="auto">
                <Button variant="danger" onClick={toggleErrorGraph}>{CLEAR_GRAPH}</Button>
              </Col>
            </Row>
            <Row>
              {
                queries && queries.map((query, index) => {
                  return (
                    <Col key={index} xs={12} md={4}>
                      <Form>
                        <Form.Group controlId='index' className='px-1 py-1'>
                          <Form.Control as='textarea'
                            disabled={queryInProgress[index]}
                            value={query}
                            rows={NUM_ROWS_IN_QUERYBOX}
                            onChange={handleQueryChange(index)}
                          />
                        </Form.Group>
                        <div style={{display: 'flex', justifyContent: 'flex-end'}}>
                          <Button variant="dark" className='mx-1 my-1'
                            onClick={onRun(index)} disabled={queryInProgress[index]}>RUN</Button>
                          <Button variant="dark" className='mx-1 my-1'
                            onClick={onQueryClear(index)} disabled={queryInProgress[index]}>CLEAR</Button>
                        </div>
                      </Form>
                      <ResultsGrid count={queryResult && queryResult[index] ? queryResult[index].count : 0}
                        cols={queryResult && queryResult[index] ? queryResult[index].cols : 0}
                      data={itemsToShow && itemsToShow[index] ? itemsToShow[index] : []}
                      duration={queryResult && queryResult[index] ? queryResult[index].queryDuration : 0} />
                      {/* <Pagination numItems={queryResult[index].count} onPageChange={onPageChange} itemsPerPage={ITEMS_PER_PAGE} /> */}
                    </Col>
                  );
                })
              }
            </Row>
            {
              showErrorGraph && (<Row>
                <ErrorGraph plotGraph={plotGraph} queryResults={queryResult} />
            </Row>)
            }
          </div>
      </div>
  );
}

export default QueryPanel;
