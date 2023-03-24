import React, { useCallback, useEffect, useRef } from "react";
import Highcharts, { extendClass } from 'highcharts';
import { Col, Row } from "react-bootstrap";

import ErrorGraphConfig from "./errorGraphConfig";

const graphStyle = {
    width: '100%',
};

const SWR = 'SWR';
const BER = 'BERNOULLI';

const ErrorGraph = ({plotGraph, queryResults}) => {
    let numGraphs;
    if (queryResults && queryResults[0]?.cols?.length > 1) {
        if(queryResults && queryResults[0]?.count === 1) {
            numGraphs = queryResults[0]?.cols?.length;
        } else {
            numGraphs = queryResults[0]?.cols?.length - 1;
        }
    } else {
        numGraphs = 1;
    }
    const graphRef = useRef(Array(numGraphs));

    const calcRelativeDiffForOne = useCallback((queryResults) => {
        if (queryResults && queryResults[0]?.cols?.length >= 1) {
            const [firstResult, secondResult, thirdResult] = queryResults;
            const {data: exactData} = firstResult;
            const {data: swrApproxData} = secondResult;
            const {data: berApproxData} = thirdResult;
            
            const exactVal = isNaN(parseInt(exactData[0])) ? 1 : parseInt(exactData[0]); // TODO: ask prof
            const swrVal = isNaN(parseInt(swrApproxData[0])) ? 0 : parseInt(swrApproxData[0]);
            const berVal = isNaN(parseInt(berApproxData[0])) ? 0 : parseInt(berApproxData[0]);
            
            const swrDiff = Math.abs(swrVal - exactVal) / exactVal * 100;
            const berDiff = Math.abs(berVal - exactVal) / exactVal * 100;
            
            const errorConfig = new ErrorGraphConfig();
            errorConfig.setTitle(`Relative error of ${firstResult.cols[0]}`);
            errorConfig.setSeries([{name : SWR, y: swrDiff}, {name: BER, y: berDiff}]);
            errorConfig.setYLim();
            return errorConfig.errorGraphConfig;
        }
    }, [plotGraph]);
    
    const calcRelativeDiffForMore = useCallback((queryResults, colNum) => {
        if (queryResults && queryResults[0]?.cols?.length >= 2) {
            const errorConfig = new ErrorGraphConfig();

            const [firstResult, secondResult, thirdResult] = queryResults;
            const {data: exactData} = firstResult;
            const {data: swrApproxData} = secondResult;
            const {data: berApproxData} = thirdResult;
            
            const rowNum = exactData.length;
            for (let row = 0; row < rowNum; row++) {
                const exactVal = isNaN(parseInt(exactData[row][colNum])) ? 1 : parseInt(exactData[row][colNum]); // TODO: ask prof
                const swrVal = isNaN(parseInt(swrApproxData[row][colNum])) ? 0 : parseInt(swrApproxData[row][colNum]);
                const berVal = isNaN(parseInt(berApproxData[row][colNum])) ? 0 : parseInt(berApproxData[row][colNum]);
                
                const swrDiff = Math.abs(swrVal - exactVal) / exactVal * 100;
                const berDiff = Math.abs(berVal - exactVal) / exactVal * 100;
                errorConfig.setSeries([{name : SWR, y: swrDiff}, {name: BER, y: berDiff}], exactData[row][0]);
            }
                
            errorConfig.setTitle(`Relative error of ${firstResult.cols[colNum]}`);
            errorConfig.setYLim();
            return errorConfig.errorGraphConfig;
        }
    }, [plotGraph]);

    useEffect(() => {
        if (graphRef.current) {
            if ((queryResults && queryResults[0]?.cols?.length > 1)) {
                if (queryResults && queryResults[0]?.count == 1) {
                    for (let index = 0; index < numGraphs; index++) {
                        const element = graphRef.current[index];
                        const graphConfig = calcRelativeDiffForMore(queryResults, index);
                        graphConfig["legend"] = {enabled: false};
                        Highcharts.chart(element, graphConfig);
                    }
                } else {
                    for (let index = 0; index < numGraphs; index++) {
                        const element = graphRef.current[index];
                        Highcharts.chart(element, calcRelativeDiffForMore(queryResults, index + 1))
                    }
                }
            } else if (queryResults && queryResults[0]?.cols?.length == 1) {
                for (let index = 0; index < numGraphs; index++) {
                    const element = graphRef.current[index];
                    const graphConfig = calcRelativeDiffForOne(queryResults, index);
                    graphConfig["legend"] = {enabled: false};
                    Highcharts.chart(element, graphConfig);
                }
            }
        }
    }, [plotGraph]);

    const getDivs = useCallback((numGraphs) => {
        const divs = [];
        for (let index = 0; index < numGraphs; index++) {
            divs.push(
                <Col key={index} xs={12} md={(numGraphs > 3 ? (12/numGraphs) : 4)}>
                    <div key={index} className="px-1 py-1 my-1 border border-1" style={graphStyle} ref={(ref) => graphRef.current[index] = ref}>
                    </div>
                </Col>
            );
        }
        return divs;
    }, [numGraphs]);

    return queryResults && queryResults.length < 3 ? null : <Row>{getDivs(numGraphs)}</Row>;
};

export default React.memo(ErrorGraph);
