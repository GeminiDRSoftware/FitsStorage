function copyCookie() {
  var cookieDiv = document.getElementById("cookie");
  var cookieText = cookieDiv.textContent;
  navigator.clipboard.writeText(cookieText);
}

function showCookie() {
  var cookieText = document.getElementById("cookie");

  if (cookieText.style.display === "none") {
    cookieText.style.display = "block";
  } else {
    cookieText.style.display = "none";
  }
}

function initConfirm() {
    // Are we logged in?
    var cookieDiv = document.getElementById("cookie");
    if (cookieDiv) {
        $(".loginlink").on("click", doConfirm);
    }
}

function doConfirm() {
    var login_methods = document.getElementById("login_methods");
    if (login_methods) {
        var login_methods_text = login_methods.textContent;
    } else {
        var login_methods_text = "Unknown";
    }
    var confirmstring = "You are already logged in and your account currently has the following authentication methods defined: ";
    confirmstring += login_methods.textContent;
    confirmstring += ". \n\nLogging in by an another method will attach the new login to the archive account you are currently logged in as.\n\n";
    confirmstring += "You should only OK this if the new login is for the same person as your current login.\n\nIf you intent is to log in as ";
    confirmstring += "a different user, Cancel this and Log Out of your current session first.";

    return confirm(confirmstring);
}

$(document).ready(initConfirm)
