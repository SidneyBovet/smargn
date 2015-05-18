/*
 * Contributors:
 *  - Valentin Rutz
 */

/**
 * Created by Valentin on 03/05/15.
 */
function display_words() {
    var words = [];
    for (var i = 1; i <= $("#words > .input-group").length; i++) {
        var word = $("#word" + i).val();
        if (word != undefined && word != "") {
            words.push(word);
        }
    }

    console.log("Displaying words: " + words.join(" "));

    $.ajax({
        type: "POST",
        url: "/display",
        data: JSON.stringify({
            "words": words
        }),
        success: function (data) {
            $("#chartContainer").css("visibility", "visible");
            // Download data.csv
            console.log(data);
            var dataCSV = "/datacsv/" + data;
            // Display
            d3.csv(dataCSV, function (data) {
                // Make graph visible
                $("#chartContainer").css("visibility", "visible");
                var svg = dimple.newSvg("#chartContainer", 1200, 720);
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
        },
        error: function (xhr) {
            $("body").html(xhr.responseText);
        },
        contentType: "application/json; charset=utf-8"
    });
}
