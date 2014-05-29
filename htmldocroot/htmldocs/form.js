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

