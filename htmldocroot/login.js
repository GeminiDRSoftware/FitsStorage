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

function initConfirm() {
    var cookieDiv = document.getElementById("cookie");
    if (cookieDiv) {
        confirm("CookieDiv");
    } else {
        confirm("NoCookieDiv");
    }
    $(".loginlink").on("click", doConfirm)
}

function doConfirm() {
    var confirmstring = "You are already logged in. Your account currently has the following authentication methods defined: ";
    // confirmstring += methods.textContent;
    confirmstring += ". Logging in by an another method will attach the new login to the archive account you are currently logged in as. ";
    // confirmstring += "You should only do this if the new login is the same person as your current login. If you intent is to log in as ";
    // confirmstring += "a different user, you should Cancel this and Log Out of your current session first."

    return confirm(confirmstring)
}

$(document).ready(initConfirm)
