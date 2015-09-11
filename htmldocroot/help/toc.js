$(document).ready(function(){
  var toc = 
    "<nav role='nagivation' class='table-of-contents'>" +
      "<h1>Contents:</h1>" +
      "<ul>";

  var newline, el, title, link;
  $("body h2").each(function() {
    el = $(this);
    title = el.text();
    link = "#" + el.attr("id");

    newline = "<li> <a href='" + link + "'>" + title + "</a></li>";
    toc += newline;
  });

  toc += "</ul></nav>";

  $("#toc").html(toc);
});
