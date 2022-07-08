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
   if (tabName === 'caltab') {
       $("div#download_all_area").hide();
   } else {
       $("div#download_all_area").show();
   }
}

function recodeUrl() {
    var recomp = "";
    urlstring = $("#things").val();
    urlstring.split("/").forEach((comp) => {
        recomp = recomp + "/" + encodeURI(comp);
    });
    return recomp;
}

function CalsTab(selected) {
    if (selected == false) {
        if ($("#caltab").html() == 'Load Associated Calibrations'
            || $("#caltab").html().startsWith('Selected')) {
            /* First time - initiate loading the associated cals */
            var allow = $("#allow_cals").val() == "yes";

            var urlstring = recodeUrl(); // $("#things").val();
            var calurl = '/associated_cals/body_only' + urlstring;
            if (allow) {
                $("#loading_cals").show();
                $('#calibration_results').load(calurl, function(){
                    $("#loading_cals").hide();
                    // we 'unmarkAll' on the associated cals once they are loaded
                    // because that's when the UI elements are present and we can
                    // attach listeners to them (unmarkAll does this work on the
                    // first call)
                    unmarkAll('associated_cals');
                    // TODO setCurrentTab should handle this, but fails silently for some reason
                    $("div#download_all_area").hide();
                    setPreviewVisibility();
                });
            } else {
                $("#not_loading_cals").show();
            }
            $("#caltab").html('View Calibrations');
        }
    } else {
        // get the list of filenames
        selection = [];
        $('.mark_customsearch:checkbox:checked').each(function() {
            selection.push(this.value);
        })

        if (selection.length == 0) {
            alert("Please select one or more files first.");
            return;
        } else {
            var urlstring = recodeUrl();
            var calurl = '/associated_cals/body_only'; // + urlstring;
            var allow = $("#allow_cals").val() == "yes";
            if (allow) {
                $("#loading_cals").show();
                $.post(calurl, data=JSON.stringify({'filelist': selection}), success=function(data){
                    $("#loading_cals").hide();
                    // we 'unmarkAll' on the associated cals once they are loaded
                    // because that's when the UI elements are present and we can
                    // attach listeners to them (unmarkAll does this work on the
                    // first call)
                    unmarkAll('associated_cals');
                    $('#calibration_results').html(data);
                    setPreviewVisibility();
                    // TODO setCurrentTab should handle this, but fails silently for some reason
                    $("div#download_all_area").hide();
                    $("#caltab").html('Selected Calibrations');
                });
            }
        }
    }
    setCurrentTab('caltab');
}

function ResultsTab() {
    setCurrentTab('resultstab');
}

function ObslogsTab() {
    if (document.getElementById("obslogstab").innerHTML == 'Load Associated Observation Logs') {
        /* First time - initiate loading the associated obslogs */
        var urlstring = recodeUrl(); // $("#things").val();
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
        $('#previewboxmain').children('img').prop('src', "/static/ajax-loading.gif");
        $('#previewbox').show();
        // Get the URL from the a href link
        var url = ($(this).children('a').prop('href'));
        var num_previews_url = url.replace('/preview/', '/num_previews/');
        $.ajax(
            {
                "url": num_previews_url
            }
        ).done(function(count) {
            // Set the URL of the img element to the preview url
            $('#previewboxmain').children('img').prop('src', url);
            // Hide it on click anywhere
            var i;
            var lnks = "";
            if (count > 1) {
                for (i=0; i<count; i++) {
                    var onclk = '$("#previewboxmain").children("img").prop("src", "' + url + '/' + i + '")';
                    lnks += " <a href=\"#\" onclick=\'" + onclk + "\'>[" + i + "]</a>";
                }
                $('#previewboxlinks').html("<p>Additonal Previews: </p>" + lnks);
            } else {
                lnks = "";
                $('#previewboxlinks').html("");
            }
            $('#previewboxmain').click(function() {
                $('#previewbox').hide();
            });
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

// Mark all the checkboxes for the given summary table
// and register/configure the "Unmark All" button to
// deselect all on that same summary table (via sumtype)
function markAll(sumtype) {
    $("input.mark_" + sumtype).prop('checked', true);
    $('#markall_' + sumtype).prop('value', 'Unmark All Files');
    $('#markall_' + sumtype).off().on('click', function(){
        unmarkAll(sumtype);
    });
};

// Unmark all the checkboxes for the given summary table
// and register/configure the Mark All button to do a
// select all on that same summary table (via sumtype)
function unmarkAll(sumtype) {
    $("input.mark_" + sumtype).prop('checked', false);
    $('#markall_' + sumtype).prop('value', 'Mark All Files');
    $('#markall_' + sumtype).off().on('click', function(){
        markAll(sumtype);
    });
    $('.mark_' + sumtype).off().on('click', {sumtype: sumtype}, handle_select);
};

// For handling range-selects in downloadables list
// see for inspiration:
// https://codepen.io/Mestika/pen/azaxQm
var lastChecked = {};
function handle_select(e){
    var sumtype = e.data.sumtype
  if(!lastChecked[sumtype]) {
    lastChecked[sumtype] = this;
    return;
  }

  if(e.shiftKey) {
    var from = $('.mark_' + sumtype).index(this);
    var to = $('.mark_' + sumtype).index(lastChecked[sumtype]);

    var start = Math.min(from, to);
    var end = Math.max(from, to) + 1;

    $('.mark_' + sumtype).slice(start, end)
      .filter(':not(:disabled)')
      .prop('checked', lastChecked[sumtype].checked);
    // countChecked();
  }
  lastChecked[sumtype] = this;
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
        CalsTab(false);
    });
    $('#obslogstab').click(function() {
        ObslogsTab();
    });
    $('#resultstab').click(function() {
        ResultsTab();
    });
    setPreviewVisibility();
    setInfoVisibility();

    // customsearch is the initially visible table, so
    // we register it in it's "Mark All" button by calling
    // unmarkAll with the 'customsearch' argument
    unmarkAll('customsearch');
});

