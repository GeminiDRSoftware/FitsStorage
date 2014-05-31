$(function() {
    $('#select').change(function() {
        if ($(this).val() == "GMOS") {
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

var progid = $("input#progid").val();
var instrument = $("input#instrument").val();
var obsclass = $("input#obs_class").val();
var obstype = $("input#obs_type").val();
var mode = $("input#Mode").val();
var filter = $("input#Filter").val();
var cntrwvl = $("input#cntrl_wvlngth").val();
var targetname = $("input#target_name").val();
var resolver = $("input#resolver").val();
var RA = $("input#RA").val();
var dec = $("input#Dec").val();
var srad = $("input#search_rad").val();
var binning = $("input#binning").val();
var disperser = $("input#disperser").val();
var mask = $("input#mask").val();

