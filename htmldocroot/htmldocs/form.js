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
        if ($(this).val() == "spectroscopy") {
            $('#Spec').slideDown();
        } else {
            $('#Spec').hide();
        }
    });
});

$(document).ready(function() {
    var urlresults = "/searchresults";
    if ($("#url").length) {
        $("#loading").slideDown();
        urlresults += $("#url").val();
        $("#searchresults").load(urlresults, function(){
        });
    };
});    
