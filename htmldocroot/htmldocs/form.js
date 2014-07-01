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
        $("#notloading").hide();
        urlresults += $("#url").val();
        $("#searchresults").load(urlresults, function(){
        });
    };
});

$('#resolver').click(function() {
});

$(function resolver() {
    var object_name = $('#object_name').val();

    if ($("#resolver").val().match("SIMBAD")) {
        var resolverget = 'http://cdsweb.u-strasbg.fr/cgi-bin/nph-sesame/-ox/S?' + object_name;
    } else if ($("#resolver").val().match("NED")) {
        var resolverget = 'http://cdsweb.u-strasbg.fr/cgi-bin/nph-sesame/-ox/N?' + object_name;
    } else if ($("#resolver").val().match("VIZIER")) {
        var resolverget = 'http://cdsweb.u-strasbg.fr/cgi-bin/nph-sesame/-ox/V?' + object_name;
    } else {
        resolverget = 'none'
        alert("Please select a resolver from the dropdown menu")
    }

    $.ajax({
        type: 'POST',
        url: resolverget,
        dataType: 'xml',
        success: function(xml) {
		     $('#ra').val($("jradeg", xml).text());
                     $('#dec').val($("jdecdeg", xml).text());
                 }
    });
});

function parseXML(xml) {
    var xmlreadout = $.parseXML(xml),
    $xml = $( xmlreadout ),
    $ra = $xml.find( "jradeg" ),
    $dec = $xml.find( "jdecdeg" )
}
