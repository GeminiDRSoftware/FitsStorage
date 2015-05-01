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

    if ($('#instselect').val().match("GPI")) {
        $('#GPI').show();
    } else {
        $('#GPI').hide();
    }
};

function setModeVisibility() {
    if (["spectroscopy", "LS", "MOS", "IFS"].indexOf($('#Mode').val()) > -1) {
        $('#Spec').show();
    } else {
        $('#Spec').hide();
    }
};

function clearInstrumentForms() {
    document.getElementById("gmos_binning").value = '';
    document.getElementById("gmos_disperser").value = '';
    document.getElementById("gmos_filter").value = '';
    document.getElementById("gmos_mask").value = '';
    document.getElementById("GMOSexpT").value = '';
    document.getElementById("NandS").value = '';
    document.getElementById("gmos_readspeed").value = '';
    document.getElementById("gmos_gain").value = '';
    document.getElementById("gmos_roi").value = '';
    document.getElementById("gnirs_disperser").value = '';
    document.getElementById("gnirs_cam").value = '';
    document.getElementById("gnirs_mask").value = '';
    document.getElementById("gnirs_depth").value = '';
    document.getElementById("gnirs_readmode").value = '';
    document.getElementById("GNIRSexpT").value = '';
    document.getElementById("niri_filter").value = '';
    document.getElementById("niri_disperser").value = '';
    document.getElementById("niri_mask").value = '';
    document.getElementById("niri_cam").value = '';
    document.getElementById("niri_readmode").value = '';
    document.getElementById("niri_roi").value = '';
    document.getElementById("NIRIexpT").value = '';
    document.getElementById("nifs_disperser").value = '';
    document.getElementById("nifs_mask").value = '';
    document.getElementById("nifs_readmode").value = '';
    document.getElementById("NIFSexpT").value = '';
    document.getElementById("michelle_filter").value = '';
    document.getElementById("michelle_disperser").value = '';
    document.getElementById("michelle_mask").value = '';
    document.getElementById("trecs_filter").value = '';
    document.getElementById("trecs_disperser").value = '';
    document.getElementById("trecs_mask").value = '';
    document.getElementById("F2_filter").value = '';
    document.getElementById("F2_disperser").value = '';
    document.getElementById("F2_mask").value = '';
    document.getElementById("F2expT").value = '';
    document.getElementById("nici_filter").value = '';
    document.getElementById("gsaoi_filter").value = '';
    document.getElementById("GSAOIexpT").value = '';
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

function CalsTab() {
    if (document.getElementById("caltab").innerHTML == 'Associated Calibrations') {
        /* already been clicked, switch to it */
        $("#searchresults").hide();
        $("#obslog_results").hide();
        $("#calibration_results").show();
        /* set the tab classes */
        document.getElementById("caltab").className += 'current';
        document.getElementById("obslogstab").className = document.getElementById("obslogstab").className.replace('current', '')
        document.getElementById("resultstab").className = document.getElementById("resultstab").className.replace('current', '')
    } else {
        /* First time - initiate loading the associated cals */
        var allow = document.getElementById("allow_cals").value;
        var urlstring = document.getElementById("things").value;
        var calurl = '/associated_cals' + urlstring;
        if (allow == "yes") {
            $("#loading_cals").show();
            $('#calibration_results').load(calurl, function(){
                $("#loading_cals").hide();
            });
        } else {
            $("#not_loading_cals").show();
        }
        document.getElementById("caltab").innerHTML='View Calibrations';
        $("#searchresults").hide();
        $("#obslog_results").hide();
        $("#calibration_results").show();
        /* set the tab classes */
        document.getElementById("caltab").className += 'current';
        document.getElementById("resultstab").className = document.getElementById("resultstab").className.replace('current', '')
        document.getElementById("obslogstab").className = document.getElementById("obslogstab").className.replace('current', '')
    }
}

function ResultsTab() {
    /* Just switch the displayed div */
    $("#calibration_results").hide();
    $("#obslog_results").hide();
    $("#searchresults").show();
    /* set the tab classes */
    document.getElementById("resultstab").className += 'current';
    document.getElementById("caltab").className = document.getElementById("caltab").className.replace('current', '')
    document.getElementById("obslogstab").className = document.getElementById("obslogstab").className.replace('current', '')
}

function ObslogsTab() {
    if (document.getElementById("obslogstab").innerHTML == 'Associated Observation Logs') {
        /* already been loaded, just switch to it */
        $("#calibration_results").hide();
        $("#searchresults").hide();
        $("#obslog_results").show();
        /* set the tab classes */
        document.getElementById("obslogstab").className += 'current';
        document.getElementById("resultstab").className = document.getElementById("resultstab").className.replace('current', '')
        document.getElementById("caltab").className = document.getElementById("caltab").className.replace('current', '')
    } else {
        /* First time - initiate loading the associated obslogs */
        var urlstring = document.getElementById("things").value;
        var obsurl = '/associated_obslogs' + urlstring;
            /* $("#loading_obslogs").show(); */
            $('#obslog_results').load(obsurl, function(){
                /* $("#loading_obslogs").hide(); */
            });
        document.getElementById("obslogstab").innerHTML='View Obslogs';
        $("#searchresults").hide();
        $("#calibration_results").hide();
        $("#obslog_results").show();
        /* set the tab classes */
        document.getElementById("obslogstab").className += 'current';
        document.getElementById("resultstab").className = document.getElementById("resultstab").className.replace('current', '')
        document.getElementById("caltab").className = document.getElementById("obslogstab").className.replace('current', '')
    }
}

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

function setPreviewVisibility() {
    $('#previewbox').hide();
    $('.preview').click(function(e) {
        e.preventDefault();
        // Set the image to the loading swirlything
        $('#previewbox').children('img').attr('src', "/htmldocs/ajax-loading.gif");
        $('#previewbox').show();
        // Get the URL from the a href link
        var url = ($(this).children('a').attr('href'));
        // Set the URL of the img element to the preview url
        $('#previewbox').children('img').attr('src', url);
        // Hide it on click anywhere
        $('#previewbox').click(function() {
            $('#previewbox').hide();
        });
    });
};


$(document).ready(function() {
    $("#loading").hide();
    $("#loading_cals").hide();
    $('#calibration_results').hide();
    setAdvancedVisibility();
    setInstVisibility();
    gmosCustomMaskEnable();
    $('#instselect').change(function() {
        setInstVisibility();
        clearInstrumentForms();
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
    $('#caltab').click(function() {
        CalsTab();
    });
    $('#obslogstab').click(function() {
        ObslogsTab();
    });
    $('#resultstab').click(function() {
        ResultsTab();
    });
    setPreviewVisibility();
});

