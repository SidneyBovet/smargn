/**
 * Created by Valentin on 26/03/15.
 */
var svg = dimple.newSvg("#chartContainer", 590, 400);
d3.csv("/output/data.csv", function (data) {
    var myChart = new dimple.chart(svg, data);
    myChart.setBounds(60, 30, 505, 305);
    var x = myChart.addCategoryAxis("x", "Year");
    x.addOrderRule("Date");
    myChart.addMeasureAxis("y", "Occurrences");
    var s = myChart.addSeries(null, dimple.plot.line);
    myChart.draw();
});