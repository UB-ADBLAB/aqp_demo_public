import Highcharts from 'highcharts';

const CAT1 = 'SWR';
const CAT2 = 'BERNOULLI';
const colors = Highcharts.getOptions().colors;

export default class ErrorGraphConfig {
    constructor() {
        this.errorGraphConfig = {
            chart: {
                type: 'column',
                height: 200
            },
            xAxis: {
                categories: [CAT1, CAT2]
            },
            yAxis: {
                title: {
                    text: 'Relative error (%)'
                },
                min: 0,
                max: 20
            },
            series: [],
            plotOptions : {
                column: {
                    dataLabels: {
                        enabled: true,
                        crop: false,
                        overflow: 'none',
                        format: '{point.y:.2f}%'
                    }
                }
            },
            tooltip: {
                pointFormat: '{point.y:.2f}%'
            }
        }
    }

    setSeries(series, seriesName) {
        // for (let index = 0; index < series.length; index++) {
        //     const element = series[index];
        // }
        this.errorGraphConfig.series.push({data: series, name:(seriesName ? seriesName : ''), color: colors[this.errorGraphConfig.series.length % 10]});
        return this.errorGraphConfig;
    }

    setTitle(title) {
        this.errorGraphConfig.title = {text: title};
        return this.errorGraphConfig;
    }

    setYLim() {
        let ymax = 0;
        for (let i = 0; i < this.errorGraphConfig.series.length; ++i) {
            const data = this.errorGraphConfig.series[i].data;
            for (let j = 0; j < data.length; ++j) {
                if (data[j].y > ymax) {
                    ymax = data[j].y;
                }
            }
        }
        
        ymax = ymax * 5;
        if (ymax < 5) {
            ymax = 5;
        }
        this.errorGraphConfig.yAxis.max = ymax;
    }
}
