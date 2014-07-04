$(function() {
    $('#instselect').change(function() {
        if ($(this).val().match("GMOS")) {
            $('#GMOS').slideDown();
        } else {
            $('#GMOS').hide();
        }
    });

    if ($('#instselect').val().match("GMOS")) {
        $('#GMOS').show();
    }

});

$(function() {
    $('#Mode').change(function() {
        if (["spectroscopy", "LS", "MOS", "IFU"].indexOf($(this).val()) > -1) {
            $('#Spec').slideDown();
        } else {
            $('#Spec').hide();
        }
    });

    if (["spectroscopy", "LS", "MOS", "IFU"].indexOf($('#Mode').val()) > -1) {
        $('#Spec').show();
    }

});

$(function() {
    $('#advanced_header').click(function() {
        $('#advanced_options').toggle();
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

$(function() {
    $('#resbutton').click(function() {
        var object_name = document.getElementById("object_name").value;

        var url = "/nameresolver"

        if (document.getElementById("resolver").value == "SIMBAD") {
            url += "/simbad/" + object_name;
        } else if (document.getElementById("resolver").value == "NED") { 
            url += "/ned/" + object_name;
        } else if (document.getElementById("resolver").value == "VIZIER") {
            url += "/vizier/" + object_name;
        } else {
            url = ''
            alert("Please select a resolver from the dropdown menu");
        };

        $.ajax({
            url: url,
            dataType: "xml",
            success: function(xml) {
                if (xml.getElementsByTagName("INFO")[0].childNodes[0].nodeValue == ' *** Nothing found *** ') {
                    alert("Object not found")
                } else {
                    ra = xml.getElementsByTagName("jradeg")[0].childNodes[0].nodeValue;
                    dec = xml.getElementsByTagName("jdedeg")[0].childNodes[0].nodeValue;
                    document.getElementById("ra").value = ra;
                    document.getElementById("dec").value = dec;
                    document.getElementById("object_name").value = '';
                }
            },
            error: function(request, status, errorThrown) {
                alert("Please provide a valid target name to resolve");
            } 
        });
    });
});
