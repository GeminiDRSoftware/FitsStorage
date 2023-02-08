/*
 * Usage note:
 *
 * This script is shared by two different kind of forms. A global variable, 'updating',
 * needs to be * set before using it.
 *
 */
var formFlags = {
  file: false,
  release: true,
  program: true,
  latestReleaseTested: null,
  allValid: function() {
    // If we're updating the date, we won't need to reupload the file
    return (updating || this.file) &&
           this.release &&
	   this.program;
  }
}

var defaultReleaseDate = null;

function setFieldValidity(field, valid) {
  var id = null;
  if (field == 'release') {
    formFlags.release = valid;
    id = '#arbRelease';
  }
  else if (field == 'program') {
    formFlags.program = valid;
    id = '#uploadProg';
  }

  if (valid)
    $(id).removeClass('invalid_data');
  else
    $(id).addClass('invalid_data');
}

function validateUsingAjax(message, field) {
  $.ajax({
  	type: "POST",
	url:  "/miscfiles/validate_add",
	data: JSON.stringify(message),
	contentType: "application/json; charset=utf-8",
	success: function(data) { setFieldValidity(field, data.result); },
	failure: function(errMsg) {
	  // TODO: what to do...?
	  alert("Uh... error?")
	}
  });
}

function validateFile() {
  var value = $('#uploadFile')[0].value;
  if (value != '') {
    formFlags.file = true;
  } else
    formFlags.file = false;
}

function validateReleaseDate() {
  var value = $('#arbRelease')[0].value;
  if (value == formFlags.latestReleaseTested)
    return;

  formFlags.latestReleaseTested = value;

  if (value == defaultReleaseDate) {
    setFieldValidity('release', true);
  } else {
    setFieldValidity('release', false);
    validateUsingAjax({ release: value }, 'release');
  }
}

function handleReleaseDateUi() {
  if ($('#uploadRelease')[0].value == 'arbitrary') {
    validateReleaseDate();
    $('#release_div').show();
    $('#arbRelease').focus();
  } else {
    setFieldValidity('release', true);
    $('#release_div').hide();
  }
}

function validateProgram() {
  var value = $('#uploadProg')[0].value;
  if (value == '') {
    setFieldValidity('program', true);
  }
  else {
    validateUsingAjax({ program: value }, 'program');
  }
}

$(document).ready(function() {
  // File stuff
  if (!updating) {
    validateFile();
    $('#uploadFile').change(validateFile);
  }

  defaultRelease = $('#arbRelease')[0].value;

  // Release date stuff
  handleReleaseDateUi();
  $('#uploadRelease').change(function() { handleReleaseDateUi(); });
  $('#arbRelease').change(function() { formFlags.release = false; });
  $('#arbRelease').focusout(validateReleaseDate);

  // Program ID stuff
  validateProgram();
  $('#uploadProg').change(function() { validateProgram(); });
});
