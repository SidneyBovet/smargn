/**
 * Created by Valentin on 26/03/15.
 */
function display_chart() {
    // Download data.csv
    $.get({
        url: "/results/" + outputFolder() + "data.csv",
        success: function (data) {
            console.log(data);
        }
    });
    // Make graph visible
    $("#chartContainer").css("visibility", "visible");
    // Display
    d3.csv("/results/" + outputFolder() + "/data.csv", function (data) {
        var svg = dimple.newSvg("#chartContainer", 590, 400);
        var myChart = new dimple.chart(svg, data);
        myChart.setBounds(10, 20, 720, 480);
        var x = myChart.addCategoryAxis("x", "Year");
        x.addOrderRule("Date");
        var y = myChart.addMeasureAxis("y", "Occurrences");
        y.showGridlines = true;
        myChart.addSeries("Word", dimple.plot.line);
        myChart.draw();
    });
}

function outputFolder() {
    var words = [];
    for (var i = 1; i <= $("#words > .input-group").length; i++) {
        var word = $("#word" + i).val();
        if (word != undefined && word != "") {
            words.push(word);
        }
    }

    var technique = $("#technique_selector").val().toString().toLowerCase();
    var parameters = [];
    for (i = 1; i <= $("#input_params > .input-group").length; i++) {
        var param = $("#parameter" + i).val();
        if (param != undefined && param != "") {
            parameters.push(param);
        }
    }
    return (words.join("-") + "_" + technique + "_" + parameters.join("-")).toString()
}