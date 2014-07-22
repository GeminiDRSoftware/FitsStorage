function setInstVisibility() {
    if ($('#instselect').val().match("GMOS")) {
        $('#GMOS').show();
    } else {
        $('#GMOS').hide();
    }

    if ($('#instselect').val().match("GNIRS")) {
        $('#GNIRS').show();
    } else {
        $('#GNIRS').hide();
    }

    if ($('#instselect').val().match("NIRI")) {
        $('#NIRI').show();
    } else {
        $('#NIRI').hide();
    }

    if ($('#instselect').val().match("NIFS")) {
        $('#NIFS').show();
    } else {
        $('#NIFS').hide();
    }

    if ($('#instselect').val().match("michelle")) {
        $('#Michelle').show();
    } else {
        $('#Michelle').hide();
    }

    if ($('#instselect').val().match("TReCS")) {
        $('#TReCS').show();
    } else {
        $('#TReCS').hide();
    }

    if ($('#instselect').val().match("F2")) {
        $('#F2').show();
    } else {
        $('#F2').hide();
    }

    if ($('#instselect').val().match("NICI")) {
        $('#NICI').show();
    } else {
        $('#NICI').hide();
    }

    if ($('#instselect').val().match("GSAOI")) {
        $('#GSAOI').show();
    } else {
        $('#GSAOI').hide();
    }
};

function setModeVisibility() {
    if (["spectroscopy", "LS", "MOS", "IFS"].indexOf($('#Mode').val()) > -1) {
        $('#Spec').show();
    } else {
        $('#Spec').hide();
    }
};

function setModeVisibility() {
    if (["spectroscopy", "LS", "MOS", "IFS"].indexOf($('#Mode').val()) > -1) {
        $('#Spec').show();
    } else {
        $('#Spec').hide();
    }
};

function setAdvancedVisibility() {
    if (document.getElementById("engdata").value != "EngExclude" || document.getElementById("svdata").value != "SvInclude" || document.getElementById("qastate").value != "NotFail") {
        $('#advanced_options').show();
    }
};

function gmosCustomMaskEnable() {
    if (document.getElementById("gmos_mask").value == "custom") {
        document.getElementById("custom_mask").disabled = false;
    } else {
        document.getElementById("custom_mask").disabled = true;
    }
};

// The Name Resolver
function nameresolver() {
    var object_name = document.getElementById("object_name").value;
    var url = "/nameresolver"
    $('#resload').show()

    if (document.getElementById("resolver").value == "SIMBAD") {
        url += "/simbad/" + object_name;
    } else if (document.getElementById("resolver").value == "NED") {
        url += "/ned/" + object_name;
    } else if (document.getElementById("resolver").value == "VIZIER") {
        url += "/vizier/" + object_name;
    } else {
        url = ''
    }

    $.ajax({
        url: url,
        dataType: "xml",
        success: function(xml, status, jqXHR) {
            if ((xml.getElementsByTagName("INFO").length > 0) && (xml.getElementsByTagName("INFO")[0].childNodes[0].nodeValue == ' *** Nothing found *** ')) {
                alert("Object not found");
                document.getElementById("ra").value = '';
                document.getElementById("dec").value = '';
                $('#resload').hide();
            } else {
                ra = xml.getElementsByTagName("jradeg")[0].childNodes[0].nodeValue;
                dec = xml.getElementsByTagName("jdedeg")[0].childNodes[0].nodeValue;
                document.getElementById("ra").value = ra;
                document.getElementById("dec").value = dec;
                $('#resload').hide();
            }
        },
        error: function(request, status, errorThrown) {
            alert("Please ensure a resolver is selected and a valid target name is specified");
            document.getElementById("ra").value = '';
            document.getElementById("dec").value = '';
            $('#resload').hide();
        }
    });
};


$(document).ready(function() {
    $("#loading").hide();
    setAdvancedVisibility();
    setInstVisibility();
    gmosCustomMaskEnable();
    $('#instselect').change(function() {
        setInstVisibility();
    });
    $('#Mode').change(function() {
        setModeVisibility();
    });
    $('#advanced_header').click(function() {
        $('#advanced_options').toggle();
    });
    $('#gmos_mask').change(function() {
        gmosCustomMaskEnable();
    });
    $('#resbutton').click(function() {
        nameresolver();
    });
});

