/**
 * Created by Valentin on 19/04/15.
 */
function adapt_params(selector) {
    var nb_params = 0;
    switch ($(selector).val()) {
        // Add the case for your technique T here. Example:
        //  case T.name:
        //      nb_params = 73;
        //      break;
        case "Naive":
        case "Inverse":
        case "Shift":
        case "DTW":
        case "Divergence":
            nb_params = 3;
            break;
        case "SmarterDivergence":
            nb_params = 3;
            break;
        default:
            nb_params = 1;
    }
    var params = $("#input_params");
    var k = params.children().size();
    if (k < nb_params) {
        for (var i = k + 1; i <= nb_params; ++i) {
            params.append(
                "<div class=\"input-group\">" +
                "<input type=\"text\" class=\"form-control\" placeholder=\"parameter" + i + "\" id=\"parameter" + i + "\">" +
                "</div>");
        }
    } else if (k > nb_params) {
        for (var j = nb_params; j < k; ++j) {
            params.children().last().remove();
        }
    }
}