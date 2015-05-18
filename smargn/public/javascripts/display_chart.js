/*
 * Contributors:
 *  - Valentin Rutz
 */

/**
 * Created by Valentin on 26/03/15.
 */
function display_chart() {
    var hash = $("#hashRes").html();
// Download data.csv
    var dataCSV = "/datacsv/" + hash;
// Display
    d3.csv(dataCSV, function (data) {
        // Make graph visible
        $("#chartContainer").css("visibility", "visible");
        var svg = dimple.newSvg("#chartContainer", 590, 400);
        var myChart = new dimple.chart(svg, data);
        myChart.setBounds(60, 30, 505, 305);
        var x = myChart.addCategoryAxis("x", "Year");
        x.addOrderRule("Date");
        var y = myChart.addMeasureAxis("y", "Occurrences");
        y.showGridlines = true;
        myChart.addSeries("Word", dimple.plot.line);
        myChart.draw();
    });
}