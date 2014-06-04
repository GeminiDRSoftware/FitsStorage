$(function() {
    $('#instselect').change(function() {
        if ($(this).val().match("GMOS")) {
            $('#GMOS').slideDown();
        } else {
            $('#GMOS').hide();
        }
    });
});

$(function() {
    $('#Mode').change(function() {
        if ($(this).val() == "Spec") {
            $('#Spec').slideDown();
        } else {
            $('#Spec').hide();
        }
    });
});

$(document).ready(function() {
    var urlresults = "/summary/";
    if ($("#url").length) {
        urlresults += $("#url").val();
        $("#searchresults").load(urlresults, function(){
        });
    };
});    
