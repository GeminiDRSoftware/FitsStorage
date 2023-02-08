var tabInfo = [
        { id: 'search_tab', divId: 'search_frame' },
        { id: 'add_tab',    divId: 'add_frame'}
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

$(document).ready(function() {
  $('#search_tab').click(function() {
    setCurrentTab('search_tab');
  });
  $('#add_tab').click(function() {
    setCurrentTab('add_tab');
  });
});
