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

$('#resbutton').click(function() {
    var object_name = document.getElementById("object_name");

    if document.getElementById("resolver").value = "SIMBAD" {
        var resolverurl = 'http://cdsweb.u-strasbg.fr/cgi-bin/nph-sesame/-ox/S?' + object_name;
    } else if document.getElementById("resolver").value = "NED" { 
        var resolverurl = 'http://cdsweb.u-strasbg.fr/cgi-bin/nph-sesame/-ox/N?' + object_name;
    } else if document.getElementById("resolver").value = "VIZIER" {
        var resolverurl = 'http://cdsweb.u-strasbg.fr/cgi-bin/nph-sesame/-ox/V?' + object_name;
    } else {
        resolverurl = 'none'
        alert("Please select a resolver from the dropdown menu")
    }
        
    if (window.XMLHttpRequest) {
        var xmlhttp = new XMLHttpRequest();
        xmlhttp.onreadystatechange = function() {
            if (xmlhttp.readyState==4 && xmlhttp.status==200) {
                ra = xmlhttp.responseXML.documentElement.getElementsByTagName("jradeg");
                dec = xmlhttp.responseXML.documentElement.getElementsByTagName("jdecdeg");
                document.getElementById("ra").innerHTML = ra;
                document.getElementById("dec").innerHTML = dec;
            } else {
                alert("Something went wrong I guess");
            }
        alert("Old browser alert");
        }
    
    xmlhttp.open("GET", resolverurl, true);
    xmlhttp.send();
    }
});

