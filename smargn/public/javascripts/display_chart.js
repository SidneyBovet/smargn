/**
 * Created by Valentin on 26/03/15.
 */
d3.csv("/assets/data/data.csv", function (data) {
    var svg = dimple.newSvg("#chartContainer", 590, 400);
    var myChart = new dimple.chart(svg, data);
    myChart.setBounds(60, 30, 505, 305);
    var x = myChart.addCategoryAxis("x", "Year");
    x.addOrderRule("Date");
    var y = myChart.addMeasureAxis("y", "Occurrences");
    y.showGridlines = true;
    var s = myChart.addSeries("Word", dimple.plot.line);
    myChart.draw();
});