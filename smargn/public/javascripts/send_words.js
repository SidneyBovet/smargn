/*
 * Contributors:
 *  - Valentin Rutz
 */

/**
 * Created by Valentin on 15/04/15.
 */
function send_words() {
    var words = [];
    for (var i = 1; i <= $("#words > .input-group").length; i++) {
        var word = $("#word" + i).val();
        if (word != undefined && word != "") {
            words.push(word);
        }
    }

    var technique = $("#technique_selector").val();
    var parameters = [];
    for (i = 1; i <= $("#input_params > .input-group").length; i++) {
        var param = $("#parameter" + i).val();
        if (param != undefined && param != "") {
            parameters.push(param);
        }
    }

    var startYear = $("#startyear").val();
    if (startYear == undefined) {
        startYear = "1840";
    }
    var endYear = $("#endyear").val();
    if (endYear == undefined) {
        endYear = "1998";
    }

    $.ajax({
        type: "POST",
        url: "/smargn",
        data: JSON.stringify({
            "words": words,
            "technique": technique,
            "parameters": parameters,
            "range": {
                "start": startYear,
                "end": endYear
            }
        }),
        success: function (data) {
            console.log(data.hash);
            var alerts = $("#errormsg");
            alerts.empty();
            if (data.notindata.length != 0) {
                data.notindata.forEach(function (w) {
                    alerts.append(
                        "<div class=\"alert alert-danger\" role=\"alert\">" +
                        w + " was not in the data."
                        + "</div>");
                });
            }
            if (data.nosimilarwords.length != 0) {
                data.nosimilarwords.forEach(function (w) {
                    alerts.append(
                        "<div class=\"alert alert-info\" role=\"alert\">" +
                        w + " has no similar words."
                        + "</div>");
                });
            }
            if (data.results != undefined) {
                var resList = $("#results");
                resList.empty();
                for (var key in data.results) {
                    if (data.results.hasOwnProperty(key)) {
                        resList.append("<li class=\"list-group-item list-group-item-success\">" + key + "</li>");
                        data.results[key].forEach(function (res) {
                            resList.append("<li class=\"list-group-item\">" + res + "</li>");
                        });
                    }
                }
            }
            $("#hashRes").html(data.hash)
            console.log($("#hashRes").html())
        },
        error: function (xhr) {
            $("body").html(xhr.responseText);
        },
        contentType: "application/json; charset=utf-8"
    });
}