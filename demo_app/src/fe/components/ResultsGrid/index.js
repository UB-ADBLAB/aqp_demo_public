import React, {
  Fragment, useState,
} from 'react';
import PropTypes from 'prop-types';
import {v4} from 'uuid';

import {Button, Collapse, Table} from 'react-bootstrap';

const ResultsGrid = ({count, cols, data, duration}) => {
  const [open, setOpen] = useState(true);

  const toggleResults = () => {
    setOpen(!open);
  };

  return data && data.length == 0 ? null :
        <Fragment>
          <span>{`showing ${count} ${count > 1 ? 'results' : 'result' }`}</span>
          {` | `}
          <span>{`time : ${(duration) / 1000.0} seconds`}</span>
          <Collapse in={open}>
            <div>
              <Table striped bordered hover>
                <thead>
                  <tr key={v4()}>
                    {cols.map((col) => <th key={v4()}>{col}</th>)}
                  </tr>
                </thead>
                <tbody>
                  {
                    data.map((row) => {
                      return (
                        <tr key={v4()}>
                          {row.map((elem) =>{
                            return <td key={v4()}>{elem}</td>;
                          })}
                        </tr>
                      );
                    })
                  }
                </tbody>
              </Table>
            </div>
          </Collapse>
        </Fragment>;
};

ResultsGrid.propType = {
  count: PropTypes.number.isRequired,
  cols: PropTypes.array.isRequired,
  data: PropTypes.object.isRequired,
};


export default React.memo(ResultsGrid);
