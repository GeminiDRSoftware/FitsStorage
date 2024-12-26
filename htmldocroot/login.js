function copyCookie() {
  var cookieDiv = document.getElementById("cookie");
  var cookieText = cookieDiv.textContent;
  navigator.clipboard.writeText(cookieText);
};

function showCookie() {
  var cookieText = document.getElementById("cookie");

  if (cookieText.style.display === "none") {
    cookieText.style.display = "block";
  } else {
    cookieText.style.display = "none";
  }
}