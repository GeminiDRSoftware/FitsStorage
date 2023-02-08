
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
    unmarkAll('raw');
});

