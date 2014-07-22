$(document).ready(function() {
        $("#loading").hide();
});

$(function() {
    $('#instselect').change(function() {
        if ($(this).val().match("GMOS")) {
            $('#GMOS').slideDown();
        } else {
            $('#GMOS').hide();
        }

        if ($(this).val().match("GNIRS")) {
            $('#GNIRS').slideDown();
        } else {
            $('#GNIRS').hide();
        }

        if ($(this).val().match("NIRI")) {
            $('#NIRI').slideDown();
        } else {
            $('#NIRI').hide();
        }

        if ($(this).val().match("NIFS")) {
            $('#NIFS').slideDown();
        } else {
            $('#NIFS').hide();
        }

        if ($(this).val().match("michelle")) {
            $('#Michelle').slideDown();
        } else {
            $('#Michelle').hide();
        }

        if ($(this).val().match("TReCS")) {
            $('#TReCS').slideDown();
        } else {
            $('#TReCS').hide();
        }

        if ($(this).val().match("F2")) {
            $('#F2').slideDown();
        } else {
            $('#F2').hide();
        }

        if ($(this).val().match("NICI")) {
            $('#NICI').slideDown();
        } else {
            $('#NICI').hide();
        }

        if ($(this).val().match("GSAOI")) {
            $('#GSAOI').slideDown();
        } else {
            $('#GSAOI').hide();
        }
    });

    if ($('#instselect').val().match("GMOS")) {
        $('#GMOS').show();
    }

    if ($('#instselect').val().match("GNIRS")) {
        $('#GNIRS').show();
    }

    if ($('#instselect').val().match("NIRI")) {
        $('#NIRI').show();
    }

    if ($('#instselect').val().match("NIFS")) {
        $('#NIFS').show();
    }

    if ($('#instselect').val().match("michelle")) {
        $('#Michelle').show();
    }

    if ($('#instselect').val().match("TReCS")) {
        $('#TReCS').show();
    }

    if ($('#instselect').val().match("F2")) {
        $('#F2').show();
    }

    if ($('#instselect').val().match("NICI")) {
        $('#NICI').show();
    }

    if ($('#instselect').val().match("GSAOI")) {
        $('#GSAOI').show();
    }
});

$(function() {
    $('#Mode').change(function() {
        if (["spectroscopy", "LS", "MOS", "IFS"].indexOf($(this).val()) > -1) {
            $('#Spec').slideDown();
        } else {
            $('#Spec').hide();
        }
    });

    if (["spectroscopy", "LS", "MOS", "IFS"].indexOf($('#Mode').val()) > -1) {
        $('#Spec').show();
    }

});

$(function() {
    if (document.getElementById("engdata").value != "EngExclude" || document.getElementById("svdata").value != "SvInclude" || document.getElementById("qastate").value != "NotFail") {
        $('#advanced_options').show();
    };
    $('#advanced_header').click(function() {
        $('#advanced_options').toggle();
    });
});

$(function() {
    if (document.getElementById("gmos_mask").value == "custom") {
        document.getElementById("custom_mask").disabled = false;
    } else {
        document.getElementById("custom_mask").disabled = true;
    };
    $('#gmos_mask').change(function() {
        if (document.getElementById("gmos_mask").value == "custom") {
            document.getElementById("custom_mask").disabled = false;
        } else {
            document.getElementById("custom_mask").disabled = true;
        }
    });
});

$(function() {
    $('#resbutton').click(function() {
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
    });
});
