function setInstVisibility() {
    var instPairs = {
    	"GMOS":     "#GMOS",
    	"GNIRS":    "#GNIRS",
    	"NIRI":     "#NIRI",
    	"NIFS":     "#NIFS",
	"michelle": "#Michelle",
    	"TReCS":    "#TReCS",
    	"F2":       "#F2",
    	"NICI":     "#NICI",
    	"GSAOI":    "#GSAOI",
    	"GPI":      "#GPI"
	};
    $.each(instPairs, function(key, value) {
        if ($('#instselect').val().match(key)) {
            $(value).show();
        } else {
            $(value).hide();
        }
    });
};

function setModeVisibility() {
    if (["spectroscopy", "LS", "MOS", "IFS"].indexOf($('#Mode').val()) > -1) {
        $('#Spec').show();
    } else {
        $('#Spec').hide();
    }
};

function clearInstrumentForms() {
    var fields_to_clear = $(".inst_clearable");
    $.each(fields_to_clear, function(index, object) {
        object.value = '';
    });
};

function setAdvancedVisibility() {
    if ($("#engdata").val() != "EngExclude" || $("#svdata").val() != "SvInclude" || $("#qastate").val() != "NotFail" || $("#filepre").val() != "") {
        $('#advanced_options').show();
    }
};

function gmosCustomMaskEnable() {
    if ($("#gmos_mask").val() == "custom") {
        $("#custom_mask").prop('disabled', false);
    } else {
        $("#custom_mask").prop('disabled', true);
    }
};


var tabInfo = [
	{ id: 'resultstab', divId: 'searchresults' },
	{ id: 'caltab',     divId: 'calibration_results'},
	{ id: 'obslogstab', divId: 'obslog_results'}
	];

function setCurrentTab(tabName) {
   $.each(tabInfo, function(index, tabObj) {
      var li = $("#" + tabObj.id);
      var frame = $("#" + tabObj.divId);
      if (tabObj.id == tabName) {
         li.prop('className', 'current');
	 frame.show()
      }
      else {
         li.prop('className', '');
	 frame.hide();
      }
   });
}

function CalsTab() {
    if ($("#caltab").html() == 'Load Associated Calibrations') {
        /* First time - initiate loading the associated cals */
        var allow = $("#allow_cals").val() == "yes";

        var urlstring = $("#things").val();
        var calurl = '/associated_cals/body_only' + urlstring;
        if (allow) {
            $("#loading_cals").show();
            $('#calibration_results').load(calurl, function(){
                $("#loading_cals").hide();
            });
        } else {
            $("#not_loading_cals").show();
        }
        $("#caltab").html('View Calibrations');
    }
    setCurrentTab('caltab');
}

function ResultsTab() {
    setCurrentTab('resultstab');
}

function ObslogsTab() {
    if (document.getElementById("obslogstab").innerHTML == 'Load Associated Observation Logs') {
        /* First time - initiate loading the associated obslogs */
        var urlstring = $("#things").val();
        var obsurl = '/associated_obslogs' + urlstring;
            /* $("#loading_obslogs").show(); */
            $('#obslog_results').load(obsurl, function(){
                /* $("#loading_obslogs").hide(); */
            });
        document.getElementById("obslogstab").innerHTML='View Obslogs';
    }
    setCurrentTab('obslogstab');
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
        dataType: "json",
        success: function(data, status, jqXHR) {
	    if (data.success) {
                $('#ra').val(data.ra);
                $('#dec').val(data.dec);
	    } else {
                alert(data.message);
                $('#ra').val('');
                $('#dec').val('');
            }
            $('#resload').hide();
        },
        error: function(request, status, errorThrown) {
            alert("Please ensure a resolver is selected and a valid target name is specified");
            $('#ra').val('');
            $('#dec').val('');
            $('#resload').hide();
        }
    });
};

function setPreviewVisibility() {
    $('#previewbox').hide();
    $('.preview').click(function(e) {
        e.preventDefault();
        // Set the image to the loading swirlything
        $('#previewbox').children('img').prop('src', "/static/ajax-loading.gif");
        $('#previewbox').show();
        // Get the URL from the a href link
        var url = ($(this).children('a').prop('href'));
        // Set the URL of the img element to the preview url
        $('#previewbox').children('img').prop('src', url);
        // Hide it on click anywhere
        $('#previewbox').click(function() {
            $('#previewbox').hide();
        });
    });
};

function setInfoVisibility() {
    $('#infobox').hide();
    $('.info').click(function(e) {
        e.preventDefault();
        // Get the URL from the a href link
        var url = ($(this).children('a').prop('href'));
        // Set the url in the open new tab link
        $('#infolink').attr('href', url);
        // Load the URL into the box using AJAX
        $('#infopayload').load(url);
        // Show the box
        $('#infobox').show();
        // Hide it on click anywhere
        $('#infobox').click(function() {
            $('#infobox').hide();
            // blank the content so it doesnt flash up next time
            $('#infopayload').empty();
        });
    });
};

function markAll() {
    $("input.mark").prop('checked', true);
    $('#markall').prop('value', 'Unmark All Files');
    $('#markall').click(function(){
        unmarkAll();
    });
};

function unmarkAll() {
    $("input.mark").prop('checked', false);
    $('#markall').prop('value', 'Mark All Files');
    $('#markall').click(function(){
        markAll();
    });
};

// For handling range-selects in downloadables list
// see for inspiration:
// https://codepen.io/Mestika/pen/azaxQm
var lastChecked = null;
function handle_select(e){
  if(!lastChecked) {
    lastChecked = this;
    return;
  }

  if(e.shiftKey) {
    var from = $('.mark').index(this);
    var to = $('.mark').index(lastChecked);

    var start = Math.min(from, to);
    var end = Math.max(from, to) + 1;

    $('.mark').slice(start, end)
      .filter(':not(:disabled)')
      .prop('checked', lastChecked.checked);
    // countChecked();
  }
  lastChecked = this;
}


$(document).ready(function() {
    $("#loading").hide();
    $("#loading_cals").hide();
    $('#calibration_results').hide();
    $('#obslog_results').hide();
    setAdvancedVisibility();
    setInstVisibility();
    setModeVisibility();
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
        $('#column_selection').hide();
    });
    $('#col_select_header').click(function() {
        $('#advanced_options').hide();
        $('#column_selection').toggle();
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
    $('.mark').click(handle_select);
    setPreviewVisibility();
    setInfoVisibility();
    unmarkAll();
});

