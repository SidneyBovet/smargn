/**
 * Created by Valentin on 15/04/15.
 */
function send_words() {
    var words = [];
    for(var i = 0; i < $(".input-group").length; i++) {
        var word = $("#word" + (i + 1)).val();
        if(word != undefined && word != "") {
            words.push(word);
        }
    }

    $.ajax({
        type: "POST",
        url: "/naive",
        data: JSON.stringify({
            "words": JSON.stringify(words)
        }),
        success: function (data) {
            var alerts = $("#errormsg");
            alerts.empty();
            if(data.notindata.length != 0) {
                data.notindata.forEach(function(w) {
                    alerts.append(
                        "<div class=\"alert alert-danger\" role=\"alert\">"+
                        w + " was not in the data."
                        +"</div>");
                });
            }
            if(data.nosimilarwords.length != 0) {
                data.nosimilarwords.forEach(function(w) {
                    alerts.append(
                        "<div class=\"alert alert-info\" role=\"alert\">"+
                        w + " has no similar words."
                        +"</div>");
                });
            }
            if(data.results != undefined) {
                var resList = $("#results");
                resList.empty();
                for(var key in data.results) {
                    if (data.results.hasOwnProperty(key)) {
                        resList.append("<li class=\"list-group-item list-group-item-success\">"+key+"</li>");
                        data.results[key].forEach(function(res) {
                            resList.append("<li class=\"list-group-item\">" + res + "</li>");
                        });
                    }
                }
            }
        },
        error: function (xhr) {
            console.log(xhr);
        },
        contentType: "application/json; charset=utf-8"
    });
}