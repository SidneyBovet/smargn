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
        var svg = dimple.newSvg("#chartContainer", "100%", 720);
        var myChart = new dimple.chart(svg, data);
        myChart.setBounds(30, 30, "100%", "100%");
        myChart.setMargins(70, 30, 30, 120);
        var x = myChart.addTimeAxis("x", "Year", null, "%Y");
        x.timePeriod = d3.time.years;
        x.timeInterval = 10;
        var y = myChart.addMeasureAxis("y", "Occurrences");
        y.showGridlines = true;
        myChart.addSeries("Word", dimple.plot.line);
        myChart.draw();
    });
}