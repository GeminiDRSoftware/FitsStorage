$(document).ready(function(){
  /* load the username pulldown */
  var whoami_url = "/whoami";
  if ($("#things").val()) {
    whoami_url += $("#things").val();
    };
  whoami_url += " #whoami"
  $("#div_whoami").load(whoami_url,function(){
  });
});
