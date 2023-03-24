/* eslint-disable max-len */
/* eslint-disable react/prop-types */
import React, {
  useEffect,
  useRef,
  Fragment,
  useState,
} from 'react';

import Highcharts from 'highcharts/highstock';
import Boost from 'highcharts/modules/boost';

// eslint-disable-next-line new-cap
Boost(Highcharts);

const Graph = (props) => {
  const graphStyle = {
    width: '100%',
  };
  const ref = useRef(null);
  let graphObjectState = useState(null);
  const [graphObject, setGraphObject] = graphObjectState;
  const maxLength = 3600;
  const [graphId, setGraphId] = useState(null);

  function onSetExtremes(e) {
    if (e.trigger !== "syncExtremes" && e.trigger !== "rangeSelectorButton") {
        props.onNavigatorChange({min: e.min, max: e.max,
                                 graphId: props.config.graphId});
    }
  }

  useEffect(() => {
    const series = props.config.seriesKeys.map((key, index) => {
      return {
        name: key,
        data: [], // Replace this with a callback function to iterate an array passed in props and extract respective series data.
        dataGrouping: {
          enabled: false,
        },
        tooltip: {
          valueDecimals: 2,
          showInNavigator: true,
        },
      };
    });
    if (ref.current) {
      setGraphObject(Highcharts.stockChart(
          ref.current,
          {
            chart: {
              animation: false,
              shadow: false,
              zooming: {
                type: 'xy',
              },
            },
            plotOptions: {
              series: {
                showInNavigator: true,
              },
            },
            yAxis: [{
              min: 0,
              lineWidth: 1,
              title: {
                text: 'Value',
              },
              opposite: false,
              tickAmount: 4,
              startOnTick: false,
              endOnTick: false,
            }],
            xAxis: {
              lineWidth: 1,
              title: {
                text: 'Timestamp',
              },
              events: {
                setExtremes: onSetExtremes
              },
            },
            accessibility: {
              enabled: false,
            },
            credits: {
              enabled: false,
            },
            time: {
              useUTC: false,
            },
            rangeSelector: {
              buttons: [
                {
                  type: 'all',
                  text: 'All',
                }
              ],
              inputEnabled: false,
              selected: 0,
            },
            title: {
              text: props.title,
            },
            exporting: {
              enabled: false,
            },
            legend: {
              enabled: true,
            },
            series: series,
          },
      ),
      );

      setGraphId(props.config.graphId);
    }
  }, []);


  useEffect(() => {
    if (props.point && props.point.length > 0) {
      if (ref.current && graphObject) {
        props.point.forEach((item, index) => {
          if ((parseInt(item.graphId) === graphId) &&
              parseInt(item.seriesId) >= 0 &&
              (parseInt(item.seriesId) < (graphObject.series.length - 1))) {
            graphObject.series[parseInt(item.seriesId)].addPoint([parseInt(item.ts), parseFloat(item.val)],
                false,
                graphObject.series[parseInt(item.seriesId)].data.length >= maxLength);
            graphObject.redraw();
          }
        });

        // Code for Testing with random data in setInterval
        // Read the value for this graphKey and add it to series.
        // graphObject.series.forEach((series, index) => {
        //   if (series.options.className != 'highcharts-navigator-series') {
        //     series.addPoint(props.point[index],
        //         false,
        //         series.data.length >= maxLength);
        //     graphObject.redraw();
        //   } else {
        //     console.log('highcharts-navigator-series ', index);
        //   }
        // });
      }
    }
  }, [props.point]);

  useEffect(() => {
    if (props.syncToggleChecked) {
      if (ref.current && graphObject) {
          if (props.navigatorEventData &&
              props.navigatorEventData.graphId !== graphId) {
                let min = props.navigatorEventData.min;
                let max = props.navigatorEventData.max;
                graphObject.xAxis[0].setExtremes(min, max,
                    undefined,
                    false, {
                      trigger: 'syncExtremes',
                    });
          }
      }
    }
  }, [props.navigatorEventData]);

  return (
    <Fragment>
      <div className = "px-1 py-1 my-1 border border-1" style={graphStyle} ref={ref}>
      </div>
    </Fragment>
  );
};

export default Graph;
